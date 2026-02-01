use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use chrono::{Datelike, Utc};
use serde_json::{Value as JsonValue, json};

use crate::trace_schema::{RuleMeta, TraceChunkRef, TraceDetailRef, TraceManifest, TraceSummary};

const DEFAULT_MAX_RECORDS_PER_CHUNK: usize = 200;
const DEFAULT_MAX_NODES_PER_CHUNK: usize = 2000;
const DEFAULT_MAX_CHUNK_BYTES: usize = 4 * 1024 * 1024; // 4MB
const DEFAULT_MAX_TRACE_BYTES: usize = 10 * 1024 * 1024; // 10MB (compressed)
const DEFAULT_SAMPLING_RATE: f64 = 1.0;
const TRACE_SCHEMA_VERSION: u8 = 1;

#[derive(Debug, Clone)]
pub struct TraceWriteOptions {
    pub max_records_per_chunk: usize,
    pub max_nodes_per_chunk: usize,
    pub max_chunk_bytes_uncompressed: usize,
    pub compression: TraceCompression,
    pub detail_level: TraceDetailLevel,
    pub max_bytes_per_trace: usize,
    pub split_nodes: bool,
    pub sampling_rate: f64,
    pub sampling_slow_threshold_us: Option<u64>,
}

impl Default for TraceWriteOptions {
    fn default() -> Self {
        Self {
            max_records_per_chunk: DEFAULT_MAX_RECORDS_PER_CHUNK,
            max_nodes_per_chunk: DEFAULT_MAX_NODES_PER_CHUNK,
            max_chunk_bytes_uncompressed: DEFAULT_MAX_CHUNK_BYTES,
            compression: TraceCompression::Zstd,
            detail_level: TraceDetailLevel::Full,
            max_bytes_per_trace: DEFAULT_MAX_TRACE_BYTES,
            split_nodes: true,
            sampling_rate: DEFAULT_SAMPLING_RATE,
            sampling_slow_threshold_us: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceDetailLevel {
    Full,
    Basic,
    Off,
}

#[derive(Debug, Clone, Copy)]
pub enum TraceCompression {
    Zstd,
    None,
}

pub async fn write_trace_bundle(
    data_dir: &Path,
    trace: &JsonValue,
    options: Option<TraceWriteOptions>,
) -> Result<PathBuf> {
    let data_dir = data_dir.to_path_buf();
    let trace = trace.clone();
    let options = options.unwrap_or_default();
    tokio::task::spawn_blocking(move || write_trace_bundle_sync(&data_dir, &trace, &options))
        .await?
}

fn write_trace_bundle_sync(
    data_dir: &Path,
    trace: &JsonValue,
    options: &TraceWriteOptions,
) -> Result<PathBuf> {
    let trace_id = trace
        .get("trace_id")
        .and_then(|value| value.as_str())
        .unwrap_or("trace")
        .to_string();
    let timestamp = trace
        .get("timestamp")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
        .unwrap_or_else(|| Utc::now().to_rfc3339());

    let (year, month, day) = parse_date_parts(&timestamp).unwrap_or_else(|| {
        let now = Utc::now();
        (now.year(), now.month(), now.day())
    });

    let trace_dir = data_dir
        .join("traces")
        .join(format!("{year:04}"))
        .join(format!("{month:02}"))
        .join(format!("{day:02}"))
        .join(&trace_id);
    fs::create_dir_all(&trace_dir)?;

    let records = trace
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    let mut record_chunks = Vec::new();
    let mut record_files = Vec::new();
    let mut node_chunks = Vec::new();
    let mut node_files = Vec::new();
    let mut finalize_chunk = None;
    let mut finalize_file = None;
    let mut detail_layout = "records_inline".to_string();

    let mut detail_level = options.detail_level;
    let mut detail_reason = Vec::new();

    if detail_level == TraceDetailLevel::Full && !should_keep_full_detail(trace, &records, options)
    {
        detail_level = TraceDetailLevel::Basic;
        detail_reason.push("sampled_out".to_string());
    }

    let mut detail_status = match detail_level {
        TraceDetailLevel::Full => "full".to_string(),
        TraceDetailLevel::Basic => "basic".to_string(),
        TraceDetailLevel::Off => "dropped".to_string(),
    };

    match detail_level {
        TraceDetailLevel::Full => {
            let (records_for_chunks, nodes_for_chunks) = if options.split_nodes {
                detail_layout = "records_nodes_split".to_string();
                split_records_and_nodes(&records)
            } else {
                (records.clone(), Vec::new())
            };

            let (chunks, files) = write_record_chunks(&trace_dir, &records_for_chunks, options)?;
            record_chunks = chunks;
            record_files = files;

            if options.split_nodes {
                let (chunks, files) = write_node_chunks(&trace_dir, &nodes_for_chunks, options)?;
                node_chunks = chunks;
                node_files = files;
            }

            let (chunk, file) = write_finalize_chunk(&trace_dir, trace, options)?;
            finalize_chunk = chunk;
            finalize_file = file;
        }
        TraceDetailLevel::Basic => {
            if detail_reason.is_empty() {
                detail_reason.push("trace_level_basic".to_string());
            }
        }
        TraceDetailLevel::Off => {
            detail_reason.push("trace_level_off".to_string());
        }
    }

    let detail_bytes = total_chunk_bytes(&record_chunks)
        .saturating_add(total_chunk_bytes(&node_chunks))
        .saturating_add(finalize_chunk.as_ref().and_then(|c| c.bytes).unwrap_or(0));
    if detail_status == "full" && detail_bytes > options.max_bytes_per_trace as u64 {
        for path in &record_files {
            let _ = fs::remove_file(path);
        }
        for path in &node_files {
            let _ = fs::remove_file(path);
        }
        if let Some(path) = finalize_file.as_ref() {
            let _ = fs::remove_file(path);
        }
        record_chunks.clear();
        record_files.clear();
        node_chunks.clear();
        node_files.clear();
        finalize_chunk = None;
        finalize_file = None;
        detail_status = "basic".to_string();
        detail_layout = "records_inline".to_string();
        detail_reason.push("budget_exceeded".to_string());
    }

    let summary = trace.get("summary").map(parse_summary);
    let rule = trace.get("rule").map(parse_rule_meta);
    let status = trace
        .get("status")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());
    let input_format = trace
        .get("input_format")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());
    let rule_source = trace.get("rule_source").cloned();

    let detail = TraceDetailRef {
        layout: detail_layout,
        status: detail_status.clone(),
        reason: detail_reason,
        records: record_chunks,
        nodes: node_chunks,
        finalize: finalize_chunk,
    };

    let manifest = TraceManifest {
        trace_schema_version: TRACE_SCHEMA_VERSION,
        trace_id: trace_id.clone(),
        timestamp: Some(timestamp),
        status,
        rule,
        input_format,
        summary,
        detail: Some(detail),
        masking: None,
        rule_source,
    };

    let manifest_path = trace_dir.join("trace.json");
    let manifest_payload = serde_json::to_string_pretty(&manifest)?;
    write_atomic(&manifest_path, manifest_payload.as_bytes())?;

    // Ensure any temporary files were created (record files already written).
    if detail_status == "full" {
        for path in record_files {
            if !path.exists() {
                return Err(anyhow::anyhow!("record chunk missing: {}", path.display()));
            }
        }
        for path in node_files {
            if !path.exists() {
                return Err(anyhow::anyhow!("node chunk missing: {}", path.display()));
            }
        }
        if let Some(path) = finalize_file {
            if !path.exists() {
                return Err(anyhow::anyhow!(
                    "finalize chunk missing: {}",
                    path.display()
                ));
            }
        }
    }

    Ok(manifest_path)
}

fn parse_date_parts(timestamp: &str) -> Option<(i32, u32, u32)> {
    let parsed = chrono::DateTime::parse_from_rfc3339(timestamp).ok()?;
    Some((parsed.year(), parsed.month(), parsed.day()))
}

fn parse_rule_meta(value: &JsonValue) -> RuleMeta {
    RuleMeta {
        name: value
            .get("name")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string()),
        path: value
            .get("path")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string()),
        r#type: value
            .get("type")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string()),
        version: value
            .get("version")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8),
    }
}

fn parse_summary(value: &JsonValue) -> TraceSummary {
    TraceSummary {
        record_total: value.get("record_total").and_then(|v| v.as_u64()),
        record_success: value.get("record_success").and_then(|v| v.as_u64()),
        record_failed: value.get("record_failed").and_then(|v| v.as_u64()),
        duration_ms: value.get("duration_ms").and_then(|v| v.as_u64()),
        duration_us: value.get("duration_us").and_then(|v| v.as_u64()),
    }
}

fn should_keep_full_detail(
    trace: &JsonValue,
    records: &[JsonValue],
    options: &TraceWriteOptions,
) -> bool {
    let rate = normalize_sampling_rate(options.sampling_rate);
    if rate >= 1.0 {
        return true;
    }
    if trace_is_error(trace, records) || trace_is_slow(trace, records, options) {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }
    let key = trace
        .get("trace_id")
        .and_then(|value| value.as_str())
        .or_else(|| trace.get("timestamp").and_then(|value| value.as_str()))
        .unwrap_or("trace");
    let bucket = sampling_bucket(key);
    bucket < rate
}

fn normalize_sampling_rate(rate: f64) -> f64 {
    if rate.is_nan() {
        return DEFAULT_SAMPLING_RATE;
    }
    rate.clamp(0.0, 1.0)
}

fn sampling_bucket(value: &str) -> f64 {
    let hash = fnv1a64(value.as_bytes());
    (hash as f64) / (u64::MAX as f64)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn trace_is_error(trace: &JsonValue, records: &[JsonValue]) -> bool {
    if let Some(status) = trace.get("status").and_then(|value| value.as_str()) {
        let status = status.to_ascii_lowercase();
        if status != "ok" && status != "success" {
            return true;
        }
    }
    if let Some(failed) = trace
        .get("summary")
        .and_then(|summary| summary.get("record_failed"))
        .and_then(|value| value.as_u64())
    {
        if failed > 0 {
            return true;
        }
    }
    records.iter().any(|record| {
        record
            .get("status")
            .and_then(|value| value.as_str())
            .map(|value| value.eq_ignore_ascii_case("error"))
            .unwrap_or(false)
    })
}

fn trace_is_slow(trace: &JsonValue, records: &[JsonValue], options: &TraceWriteOptions) -> bool {
    let Some(threshold) = options.sampling_slow_threshold_us else {
        return false;
    };
    trace_duration_us(trace, records)
        .map(|duration| duration >= threshold)
        .unwrap_or(false)
}

fn trace_duration_us(trace: &JsonValue, records: &[JsonValue]) -> Option<u64> {
    if let Some(duration) = trace
        .get("summary")
        .and_then(|summary| summary.get("duration_us"))
        .and_then(|value| value.as_u64())
    {
        return Some(duration);
    }
    if let Some(duration) = trace
        .get("summary")
        .and_then(|summary| summary.get("duration_ms"))
        .and_then(|value| value.as_u64())
    {
        return Some(duration.saturating_mul(1000));
    }
    if let Some(duration) = trace.get("duration_us").and_then(|value| value.as_u64()) {
        return Some(duration);
    }
    if let Some(duration) = trace.get("duration_ms").and_then(|value| value.as_u64()) {
        return Some(duration.saturating_mul(1000));
    }

    let mut total = 0u64;
    let mut found = false;
    for record in records {
        if let Some(duration) = record.get("duration_us").and_then(|value| value.as_u64()) {
            total = total.saturating_add(duration);
            found = true;
        } else if let Some(duration) = record.get("duration_ms").and_then(|value| value.as_u64()) {
            total = total.saturating_add(duration.saturating_mul(1000));
            found = true;
        }
    }

    if found { Some(total) } else { None }
}

fn split_records_and_nodes(records: &[JsonValue]) -> (Vec<JsonValue>, Vec<JsonValue>) {
    let mut records_out = Vec::with_capacity(records.len());
    let mut nodes_out = Vec::new();

    for (index, record) in records.iter().enumerate() {
        let record_index = record
            .get("index")
            .and_then(|value| value.as_u64())
            .unwrap_or(index as u64);
        if let Some(nodes) = record.get("nodes").and_then(|value| value.as_array()) {
            for node in nodes {
                nodes_out.push(json!({
                    "record_index": record_index,
                    "node": node
                }));
            }
        }

        let mut record_clone = record.clone();
        if let Some(obj) = record_clone.as_object_mut() {
            if obj
                .get("nodes")
                .and_then(|value| value.as_array())
                .is_some()
            {
                obj.remove("nodes");
            }
        }
        records_out.push(record_clone);
    }

    (records_out, nodes_out)
}

fn write_record_chunks(
    trace_dir: &Path,
    records: &[JsonValue],
    options: &TraceWriteOptions,
) -> Result<(Vec<TraceChunkRef>, Vec<PathBuf>)> {
    let mut chunks = Vec::new();
    let mut files = Vec::new();

    let mut chunk_index = 0usize;
    let mut record_start = 0usize;
    let mut current_lines: Vec<String> = Vec::new();
    let mut current_bytes: usize = 0;

    let mut flush = |lines: &mut Vec<String>, start: usize, end: usize| -> Result<()> {
        if lines.is_empty() {
            return Ok(());
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
        let payload = format!("{}\n", lines.join("\n"));
        let raw_bytes = payload.as_bytes();

        match options.compression {
            TraceCompression::Zstd => {
                let compressed = zstd::stream::encode_all(raw_bytes, 3)?;
                write_atomic(&path, compressed.as_slice())?;
            }
            TraceCompression::None => {
                write_atomic(&path, raw_bytes)?;
            }
        }

        let bytes = fs::metadata(&path).ok().map(|meta| meta.len());
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
            bytes,
        });
        files.push(path);
        lines.clear();
        Ok(())
    };

    for (index, record) in records.iter().enumerate() {
        let line = serde_json::to_string(record)
            .with_context(|| format!("failed to serialize record at {index}"))?;
        let line_len = line.as_bytes().len() + 1; // newline
        let exceeds_record_limit = lines_len_exceeds(&current_lines, options.max_records_per_chunk);
        let exceeds_byte_limit = current_bytes + line_len > options.max_chunk_bytes_uncompressed;
        if !current_lines.is_empty() && (exceeds_record_limit || exceeds_byte_limit) {
            let end = record_start + current_lines.len() - 1;
            flush(&mut current_lines, record_start, end)?;
            record_start = index;
            current_bytes = 0;
        }
        current_bytes += line_len;
        current_lines.push(line);
    }

    if !current_lines.is_empty() {
        let end = record_start + current_lines.len() - 1;
        flush(&mut current_lines, record_start, end)?;
    }

    Ok((chunks, files))
}

fn write_node_chunks(
    trace_dir: &Path,
    nodes: &[JsonValue],
    options: &TraceWriteOptions,
) -> Result<(Vec<TraceChunkRef>, Vec<PathBuf>)> {
    if nodes.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut chunks = Vec::new();
    let mut files = Vec::new();

    let mut chunk_index = 0usize;
    let mut node_start = 0usize;
    let mut current_lines: Vec<String> = Vec::new();
    let mut current_bytes: usize = 0;

    let mut flush = |lines: &mut Vec<String>, start: usize, end: usize| -> Result<()> {
        if lines.is_empty() {
            return Ok(());
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
        let payload = format!("{}\n", lines.join("\n"));
        let raw_bytes = payload.as_bytes();

        match options.compression {
            TraceCompression::Zstd => {
                let compressed = zstd::stream::encode_all(raw_bytes, 3)?;
                write_atomic(&path, compressed.as_slice())?;
            }
            TraceCompression::None => {
                write_atomic(&path, raw_bytes)?;
            }
        }

        let bytes = fs::metadata(&path).ok().map(|meta| meta.len());
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
            bytes,
        });
        files.push(path);
        lines.clear();
        Ok(())
    };

    for (index, node) in nodes.iter().enumerate() {
        let line = serde_json::to_string(node)
            .with_context(|| format!("failed to serialize node at {index}"))?;
        let line_len = line.as_bytes().len() + 1; // newline
        let exceeds_node_limit = lines_len_exceeds(&current_lines, options.max_nodes_per_chunk);
        let exceeds_byte_limit = current_bytes + line_len > options.max_chunk_bytes_uncompressed;
        if !current_lines.is_empty() && (exceeds_node_limit || exceeds_byte_limit) {
            let end = node_start + current_lines.len() - 1;
            flush(&mut current_lines, node_start, end)?;
            node_start = index;
            current_bytes = 0;
        }
        current_bytes += line_len;
        current_lines.push(line);
    }

    if !current_lines.is_empty() {
        let end = node_start + current_lines.len() - 1;
        flush(&mut current_lines, node_start, end)?;
    }

    Ok((chunks, files))
}

fn lines_len_exceeds(lines: &[String], max: usize) -> bool {
    lines.len() >= max && max > 0
}

fn total_chunk_bytes(chunks: &[TraceChunkRef]) -> u64 {
    chunks.iter().filter_map(|chunk| chunk.bytes).sum()
}

fn write_finalize_chunk(
    trace_dir: &Path,
    trace: &JsonValue,
    options: &TraceWriteOptions,
) -> Result<(Option<TraceChunkRef>, Option<PathBuf>)> {
    let finalize = match trace.get("finalize") {
        Some(value) => value,
        None => return Ok((None, None)),
    };

    let filename = format!(
        "finalize.json{}",
        match options.compression {
            TraceCompression::Zstd => ".zst",
            TraceCompression::None => "",
        }
    );
    let path = trace_dir.join(&filename);
    let payload = serde_json::to_vec(finalize)?;
    let raw_bytes = payload.as_slice();
    match options.compression {
        TraceCompression::Zstd => {
            let compressed = zstd::stream::encode_all(raw_bytes, 3)?;
            write_atomic(&path, compressed.as_slice())?;
        }
        TraceCompression::None => {
            write_atomic(&path, raw_bytes)?;
        }
    }
    let bytes = fs::metadata(&path).ok().map(|meta| meta.len());
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
        bytes,
    };
    Ok((Some(chunk), Some(path)))
}

fn write_atomic(path: &Path, payload: &[u8]) -> Result<()> {
    let temp_path = temp_path_for(path)?;
    fs::write(&temp_path, payload)
        .with_context(|| format!("failed to write temporary file: {}", temp_path.display()))?;
    if let Err(err) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(anyhow::anyhow!(
            "failed to rename temp file {} -> {}: {}",
            temp_path.display(),
            path.display(),
            err
        ));
    }
    Ok(())
}

fn temp_path_for(path: &Path) -> Result<PathBuf> {
    let filename = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("missing file name for trace chunk"))?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id();
    let temp_name = format!("{}.tmp-{pid}-{nanos}", filename.to_string_lossy());
    Ok(path.with_file_name(temp_name))
}
