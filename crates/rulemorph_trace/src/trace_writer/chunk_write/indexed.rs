use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::Value as JsonValue;

use crate::trace_schema::TraceChunkRef;

use super::super::{TraceCompression, TraceWriteOptions, reserve_budget, write_atomic};

pub(in crate::trace_writer) struct ChunkWriteResult {
    pub(in crate::trace_writer) chunks: Vec<TraceChunkRef>,
    pub(in crate::trace_writer) files: Vec<PathBuf>,
    pub(in crate::trace_writer) budget_exceeded: bool,
}

enum IndexedChunkKind {
    Record,
    Node,
}

impl IndexedChunkKind {
    fn label(&self) -> &'static str {
        match self {
            Self::Record => "record",
            Self::Node => "node",
        }
    }

    fn filename(&self, chunk_index: usize, compression: TraceCompression) -> String {
        let prefix = match self {
            Self::Record => "records",
            Self::Node => "nodes",
        };
        format!(
            "{prefix}-{chunk_index:04}.ndjson{}",
            match compression {
                TraceCompression::Zstd => ".zst",
                TraceCompression::None => "",
            }
        )
    }

    fn chunk_ref(
        &self,
        filename: String,
        compression: TraceCompression,
        start: usize,
        end: usize,
        bytes: u64,
        bytes_uncompressed: u64,
    ) -> TraceChunkRef {
        TraceChunkRef {
            path: filename,
            format: "ndjson".to_string(),
            compression: match compression {
                TraceCompression::Zstd => "zstd".to_string(),
                TraceCompression::None => "none".to_string(),
            },
            record_start: match self {
                Self::Record => Some(start as u64),
                Self::Node => None,
            },
            record_end: match self {
                Self::Record => Some(end as u64),
                Self::Node => None,
            },
            node_start: match self {
                Self::Record => None,
                Self::Node => Some(start as u64),
            },
            node_end: match self {
                Self::Record => None,
                Self::Node => Some(end as u64),
            },
            bytes: Some(bytes),
            bytes_uncompressed: Some(bytes_uncompressed),
        }
    }
}

pub(in crate::trace_writer) fn write_record_chunks(
    trace_dir: &Path,
    records: &[JsonValue],
    options: &TraceWriteOptions,
    budget_remaining: &mut u64,
    chunk_budget_remaining: &mut usize,
) -> Result<ChunkWriteResult> {
    write_indexed_chunks(
        trace_dir,
        records,
        IndexedChunkKind::Record,
        options.max_records_per_chunk,
        options,
        budget_remaining,
        chunk_budget_remaining,
    )
}

pub(in crate::trace_writer) fn write_node_chunks(
    trace_dir: &Path,
    nodes: &[JsonValue],
    options: &TraceWriteOptions,
    budget_remaining: &mut u64,
    chunk_budget_remaining: &mut usize,
) -> Result<ChunkWriteResult> {
    write_indexed_chunks(
        trace_dir,
        nodes,
        IndexedChunkKind::Node,
        options.max_nodes_per_chunk,
        options,
        budget_remaining,
        chunk_budget_remaining,
    )
}

fn write_indexed_chunks(
    trace_dir: &Path,
    items: &[JsonValue],
    kind: IndexedChunkKind,
    max_items_per_chunk: usize,
    options: &TraceWriteOptions,
    budget_remaining: &mut u64,
    chunk_budget_remaining: &mut usize,
) -> Result<ChunkWriteResult> {
    let mut chunks = Vec::new();
    let mut files = Vec::new();
    let mut budget_exceeded = false;

    let mut chunk_index = 0usize;
    let mut item_start = 0usize;
    let mut current_lines: Vec<String> = Vec::new();
    let mut current_bytes: usize = 0;

    let mut flush = |lines: &mut Vec<String>, start: usize, end: usize| -> Result<bool> {
        if lines.is_empty() {
            return Ok(false);
        }
        chunk_index += 1;
        let filename = kind.filename(chunk_index, options.compression);
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
        chunks.push(kind.chunk_ref(
            filename,
            options.compression,
            start,
            end,
            bytes,
            raw_bytes.len() as u64,
        ));
        files.push(path);
        lines.clear();
        Ok(false)
    };

    for (index, item) in items.iter().enumerate() {
        let label = kind.label();
        let line = serde_json::to_string(item)
            .with_context(|| format!("failed to serialize {label} at {index}"))?;
        let line_len = line.as_bytes().len() + 1; // newline
        let exceeds_item_limit = lines_len_exceeds(&current_lines, max_items_per_chunk);
        let exceeds_byte_limit = current_bytes + line_len > options.max_chunk_bytes_uncompressed;
        if !current_lines.is_empty() && (exceeds_item_limit || exceeds_byte_limit) {
            let end = item_start + current_lines.len() - 1;
            if flush(&mut current_lines, item_start, end)? {
                budget_exceeded = true;
                break;
            }
            item_start = index;
            current_bytes = 0;
        }
        current_bytes += line_len;
        current_lines.push(line);
    }

    if !budget_exceeded && !current_lines.is_empty() {
        let end = item_start + current_lines.len() - 1;
        if flush(&mut current_lines, item_start, end)? {
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
