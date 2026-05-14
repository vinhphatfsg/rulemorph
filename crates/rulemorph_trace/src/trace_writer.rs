use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use chrono::{Datelike, Utc};
use serde_json::Value as JsonValue;
use tracing::warn;

mod atomic;
mod chunk_write;
mod externalize;
mod masking;
mod queue;
#[cfg(test)]
mod queue_tests;
mod record_nodes;
mod sampling;
#[cfg(test)]
mod write_failure_tests;

#[cfg(test)]
use atomic::fail_write_for_trace_id;
use atomic::write_atomic;
use chunk_write::{
    max_ndjson_line_bytes, total_chunk_bytes, write_finalize_chunk, write_node_chunks,
    write_record_chunks,
};
use externalize::externalize_trace_payloads;
use masking::{apply_masking, normalize_masking_rules};
use queue::{
    TraceQueue, TraceWriteRequest, estimate_trace_bytes, evict_normal, push_request, queue_bytes,
    trace_id_for_log, trace_writer_loop,
};
use record_nodes::{normalize_inline_records, split_records_and_nodes};
use sampling::{TracePriority, should_keep_full_detail, trace_priority};

use crate::trace_backend::TraceWriteBackend;
use crate::trace_id::{sanitize_trace_id, trace_id_is_placeholder};
use crate::trace_schema::{
    RuleMeta, TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TRACE_CHUNK_COUNT_HARD_MAX,
    TRACE_JSON_MAX_BYTES, TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX, TraceDetailRef,
    TraceManifest, TraceMasking, TraceSummary,
};

const DEFAULT_MAX_RECORDS_PER_CHUNK: usize = 200;
const DEFAULT_MAX_NODES_PER_CHUNK: usize = 2000;
const DEFAULT_MAX_CHUNK_BYTES: usize = 4 * 1024 * 1024; // 4MB
const DEFAULT_MAX_TRACE_BYTES: usize = 10 * 1024 * 1024; // 10MB (compressed)
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 64 * 1024;
const DEFAULT_PAYLOAD_PREVIEW_BYTES: usize = 1024;
const DEFAULT_SAMPLING_RATE: f64 = 1.0;
const DEFAULT_TRACE_QUEUE_CAPACITY: usize = 256;
const DEFAULT_TRACE_QUEUE_MAX_BYTES: usize = 64 * 1024 * 1024;
const TRACE_SCHEMA_VERSION: u8 = 1;

fn clamp_max_chunk_bytes_uncompressed(value: usize) -> usize {
    value.clamp(1, TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX)
}

fn append_detail_reason(existing: Option<String>, reason: &str) -> Option<String> {
    match existing {
        None => Some(reason.to_string()),
        Some(current) => {
            let already_present = current
                .split(|ch| ch == ',' || ch == ';')
                .any(|item| item.trim() == reason);
            if already_present {
                Some(current)
            } else {
                Some(format!("{current},{reason}"))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TraceWriteOptions {
    pub max_records_per_chunk: usize,
    pub max_nodes_per_chunk: usize,
    pub max_chunk_bytes_uncompressed: usize,
    pub compression: TraceCompression,
    pub detail_level: TraceDetailLevel,
    pub max_bytes_per_trace: usize,
    pub max_payload_bytes: usize,
    pub payload_preview_bytes: usize,
    pub masking_enabled: bool,
    pub masking_rules: Vec<String>,
    pub detail_reason: Option<String>,
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
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            payload_preview_bytes: DEFAULT_PAYLOAD_PREVIEW_BYTES,
            masking_enabled: true,
            masking_rules: vec![
                "password".to_string(),
                "token".to_string(),
                "secret".to_string(),
                "authorization".to_string(),
                "cookie".to_string(),
                "api_key".to_string(),
                "api-key".to_string(),
                "apikey".to_string(),
            ],
            detail_reason: None,
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

#[derive(Clone)]
pub struct TraceWriterConfig {
    pub queue_capacity: usize,
    pub queue_max_bytes: usize,
    pub write_options: TraceWriteOptions,
    pub spawn_worker: bool,
    pub write_backend: Option<Arc<dyn TraceWriteBackend>>,
}

impl Default for TraceWriterConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_TRACE_QUEUE_CAPACITY,
            queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
            write_options: TraceWriteOptions::default(),
            spawn_worker: true,
            write_backend: None,
        }
    }
}

#[derive(Clone)]
struct FileTraceWriteBackend {
    data_dir: PathBuf,
}

impl FileTraceWriteBackend {
    fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }
}

impl TraceWriteBackend for FileTraceWriteBackend {
    fn write_trace_bundle(&self, trace: &JsonValue, options: &TraceWriteOptions) -> Result<()> {
        write_trace_bundle_sync(&self.data_dir, trace, options).map(|_| ())
    }
}

#[derive(Clone)]
pub struct TraceWriter {
    queue: Arc<TraceQueue>,
    default_options: TraceWriteOptions,
}

impl TraceWriter {
    pub fn new(data_dir: PathBuf) -> Self {
        Self::with_config(data_dir, TraceWriterConfig::default())
    }

    pub fn with_config(data_dir: PathBuf, config: TraceWriterConfig) -> Self {
        let backend = config
            .write_backend
            .unwrap_or_else(|| Arc::new(FileTraceWriteBackend::new(data_dir)));
        let queue = Arc::new(TraceQueue::new(
            backend,
            config.queue_capacity,
            config.queue_max_bytes,
        ));
        if config.spawn_worker {
            let worker_queue = queue.clone();
            std::thread::spawn(move || trace_writer_loop(worker_queue));
        }
        Self {
            queue,
            default_options: config.write_options,
        }
    }

    pub fn enqueue(&self, trace: JsonValue) -> bool {
        self.enqueue_with_options(trace, None)
    }

    pub fn enqueue_with_options(
        &self,
        trace: JsonValue,
        options: Option<TraceWriteOptions>,
    ) -> bool {
        let options = options.unwrap_or_else(|| self.default_options.clone());
        let priority = trace_priority(&trace, &options);
        let mut request = TraceWriteRequest {
            trace,
            options,
            priority,
            downgraded: false,
            approx_bytes: 0,
        };
        if request.options.detail_level != TraceDetailLevel::Full {
            strip_trace_detail(&mut request.trace);
        }
        if request.options.masking_enabled {
            let masking_rules = normalize_masking_rules(&request.options.masking_rules);
            apply_masking(&mut request.trace, &masking_rules);
        }
        request.approx_bytes = estimate_trace_bytes(&request.trace);
        let mut guard = self.queue.items.lock().expect("trace queue lock");
        let mut current_bytes = queue_bytes(&guard);
        let mut queue_full = guard.len() >= self.queue.capacity
            || current_bytes.saturating_add(request.approx_bytes) > self.queue.max_bytes;

        if queue_full && priority == TracePriority::High {
            while guard.len() >= self.queue.capacity
                || current_bytes.saturating_add(request.approx_bytes) > self.queue.max_bytes
            {
                if !evict_normal(&mut guard) {
                    break;
                }
                current_bytes = queue_bytes(&guard);
            }
            queue_full = guard.len() >= self.queue.capacity
                || current_bytes.saturating_add(request.approx_bytes) > self.queue.max_bytes;
        }

        if queue_full && request.options.detail_level == TraceDetailLevel::Full {
            request.options.detail_level = TraceDetailLevel::Basic;
            request.options.detail_reason =
                append_detail_reason(request.options.detail_reason.take(), "queue_full");
            request.downgraded = true;
            strip_trace_detail(&mut request.trace);
            request.approx_bytes = estimate_trace_bytes(&request.trace);
            queue_full = guard.len() >= self.queue.capacity
                || current_bytes.saturating_add(request.approx_bytes) > self.queue.max_bytes;

            if queue_full && priority == TracePriority::High {
                while guard.len() >= self.queue.capacity
                    || current_bytes.saturating_add(request.approx_bytes) > self.queue.max_bytes
                {
                    if !evict_normal(&mut guard) {
                        break;
                    }
                    current_bytes = queue_bytes(&guard);
                }
                queue_full = guard.len() >= self.queue.capacity
                    || current_bytes.saturating_add(request.approx_bytes) > self.queue.max_bytes;
            }
        }

        if queue_full {
            let can_enqueue = match priority {
                TracePriority::High => {
                    guard.len() < self.queue.capacity
                        && current_bytes.saturating_add(request.approx_bytes)
                            <= self.queue.max_bytes
                }
                TracePriority::Normal => false,
            };
            if !can_enqueue {
                warn!(
                    "trace queue full; dropping trace {}",
                    trace_id_for_log(&request.trace)
                );
                return false;
            }
        }
        push_request(&mut guard, request);
        self.queue.cvar.notify_one();
        true
    }
}

fn strip_trace_detail(trace: &mut JsonValue) {
    let Some(obj) = trace.as_object_mut() else {
        return;
    };
    obj.remove("records");
    obj.remove("finalize");
    obj.remove("nodes");
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
    let mut trace = trace.clone();
    let mut options = options.clone();
    options.max_chunk_bytes_uncompressed =
        clamp_max_chunk_bytes_uncompressed(options.max_chunk_bytes_uncompressed);
    let raw_trace_id = trace
        .get("trace_id")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let fallback = format!("trace-{nanos}");
            warn!("trace_id missing; using generated id {}", fallback);
            fallback
        });
    let mut trace_id = sanitize_trace_id(&raw_trace_id);
    let trace_id_is_placeholder = trace_id_is_placeholder(&trace_id);
    if trace_id.is_empty() || trace_id_is_placeholder {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        trace_id = format!("trace-{nanos}");
        if trace_id_is_placeholder {
            warn!("trace_id is insufficient; using generated id {}", trace_id);
        } else {
            warn!(
                "trace_id sanitized to empty; using generated id {}",
                trace_id
            );
        }
    } else if trace_id != raw_trace_id {
        warn!("trace_id sanitized from {} to {}", raw_trace_id, trace_id);
    }
    let timestamp = trace
        .get("timestamp")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
        .unwrap_or_else(|| Utc::now().to_rfc3339());

    let (year, month, day) = parse_date_parts(&timestamp).unwrap_or_else(|| {
        let now = Utc::now();
        (now.year(), now.month(), now.day())
    });

    let trace_dir_base = data_dir
        .join("traces")
        .join(format!("{year:04}"))
        .join(format!("{month:02}"))
        .join(format!("{day:02}"));
    let (trace_id, trace_dir) = ensure_unique_trace_dir(&trace_dir_base, trace_id, &raw_trace_id)?;
    if let Some(obj) = trace.as_object_mut() {
        obj.insert("trace_id".to_string(), JsonValue::String(trace_id.clone()));
    }
    let mut trace_dir_guard = TraceDirGuard::new(trace_dir.clone());

    let records_raw = trace
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
    if let Some(reason) = options.detail_reason.as_ref() {
        for item in reason.split(|ch| ch == ',' || ch == ';') {
            let trimmed = item.trim();
            if trimmed.is_empty() {
                continue;
            }
            if !detail_reason.iter().any(|existing| existing == trimmed) {
                detail_reason.push(trimmed.to_string());
            }
        }
    }

    if detail_level == TraceDetailLevel::Full
        && !should_keep_full_detail(&trace, &records_raw, &options)
    {
        detail_level = TraceDetailLevel::Basic;
        detail_reason.push("sampled_out".to_string());
    }

    let mut detail_status = match detail_level {
        TraceDetailLevel::Full => "full".to_string(),
        TraceDetailLevel::Basic => "basic".to_string(),
        TraceDetailLevel::Off => "dropped".to_string(),
    };

    let masking_rules = if options.masking_enabled {
        normalize_masking_rules(&options.masking_rules)
    } else {
        Vec::new()
    };
    let masking = if options.masking_enabled {
        Some(TraceMasking {
            enabled: true,
            rules: masking_rules.clone(),
        })
    } else {
        None
    };
    if detail_level != TraceDetailLevel::Full {
        strip_trace_detail(&mut trace);
    }
    if options.masking_enabled {
        apply_masking(&mut trace, &masking_rules);
    }

    let mut blob_files: Vec<PathBuf> = Vec::new();
    let mut budget_remaining = options.max_bytes_per_trace as u64;
    let mut chunk_budget_remaining = TRACE_CHUNK_COUNT_HARD_MAX;
    let mut budget_exceeded = false;
    let mut chunk_too_large = false;
    if detail_level == TraceDetailLevel::Full {
        budget_exceeded = externalize_trace_payloads(
            &mut trace,
            &trace_dir,
            &options,
            &mut budget_remaining,
            &mut blob_files,
        )?;
    }

    match detail_level {
        TraceDetailLevel::Full => {
            if !budget_exceeded {
                let records = trace
                    .get("records")
                    .and_then(|value| value.as_array())
                    .cloned()
                    .unwrap_or_default();
                let (records_for_chunks, nodes_for_chunks) = if options.split_nodes {
                    detail_layout = "records_nodes_split".to_string();
                    split_records_and_nodes(&records)
                } else {
                    (normalize_inline_records(&records), Vec::new())
                };

                let total_records = records_for_chunks.len();
                let total_nodes = if options.split_nodes {
                    nodes_for_chunks.len()
                } else {
                    count_inline_nodes(&records_for_chunks, TRACE_NODE_COUNT_HARD_MAX)
                };
                if total_records > TRACE_RECORD_COUNT_HARD_MAX
                    || total_nodes > TRACE_NODE_COUNT_HARD_MAX
                {
                    budget_exceeded = true;
                }

                if !budget_exceeded {
                    let max_record_line = max_ndjson_line_bytes(&records_for_chunks, "record")?;
                    let max_node_line = max_ndjson_line_bytes(&nodes_for_chunks, "node")?;
                    let max_line = max_record_line.max(max_node_line);
                    if max_line > options.max_chunk_bytes_uncompressed {
                        detail_status = "basic".to_string();
                        detail_layout = "records_inline".to_string();
                        if !detail_reason
                            .iter()
                            .any(|reason| reason == "chunk_too_large")
                        {
                            detail_reason.push("chunk_too_large".to_string());
                        }
                        chunk_too_large = true;
                    } else {
                        let record_result = write_record_chunks(
                            &trace_dir,
                            &records_for_chunks,
                            &options,
                            &mut budget_remaining,
                            &mut chunk_budget_remaining,
                        )?;
                        record_chunks = record_result.chunks;
                        record_files = record_result.files;
                        budget_exceeded = record_result.budget_exceeded;

                        if !budget_exceeded && options.split_nodes {
                            let node_result = write_node_chunks(
                                &trace_dir,
                                &nodes_for_chunks,
                                &options,
                                &mut budget_remaining,
                                &mut chunk_budget_remaining,
                            )?;
                            node_chunks = node_result.chunks;
                            node_files = node_result.files;
                            budget_exceeded = node_result.budget_exceeded;
                        }

                        if !budget_exceeded {
                            let finalize_result = write_finalize_chunk(
                                &trace_dir,
                                &trace,
                                &options,
                                &mut budget_remaining,
                                &mut chunk_budget_remaining,
                            )?;
                            finalize_chunk = finalize_result.chunk;
                            finalize_file = finalize_result.file;
                            budget_exceeded = finalize_result.budget_exceeded;
                            if finalize_result.size_exceeded {
                                chunk_too_large = true;
                            }
                        }
                    }
                }
            }
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

    if budget_exceeded || chunk_too_large {
        cleanup_detail_files(
            &mut record_files,
            &mut node_files,
            &mut finalize_file,
            &mut blob_files,
        );
        record_chunks.clear();
        node_chunks.clear();
        finalize_chunk = None;
        detail_status = "basic".to_string();
        detail_layout = "records_inline".to_string();
        if budget_exceeded
            && !detail_reason
                .iter()
                .any(|reason| reason == "budget_exceeded")
        {
            detail_reason.push("budget_exceeded".to_string());
        }
        if chunk_too_large
            && !detail_reason
                .iter()
                .any(|reason| reason == "chunk_too_large")
        {
            detail_reason.push("chunk_too_large".to_string());
        }
    }

    let detail_bytes = total_chunk_bytes(&record_chunks)
        .saturating_add(total_chunk_bytes(&node_chunks))
        .saturating_add(finalize_chunk.as_ref().and_then(|c| c.bytes).unwrap_or(0))
        .saturating_add(blob_total_bytes(&blob_files));
    if detail_status == "full" && detail_bytes > options.max_bytes_per_trace as u64 {
        cleanup_detail_files(
            &mut record_files,
            &mut node_files,
            &mut finalize_file,
            &mut blob_files,
        );
        record_chunks.clear();
        node_chunks.clear();
        finalize_chunk = None;
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
    let mut rule_source = trace.get("rule_source").cloned();
    if let Some(rule_source_value) = rule_source.as_ref() {
        let rule_source_bytes = serde_json::to_vec(rule_source_value)
            .map(|payload| payload.len() as u64)
            .unwrap_or(0);
        if rule_source_bytes > options.max_bytes_per_trace as u64 {
            rule_source = None;
            if !detail_reason
                .iter()
                .any(|reason| reason == "rule_source_dropped")
            {
                detail_reason.push("rule_source_dropped".to_string());
            }
        }
    }

    let mut detail = TraceDetailRef {
        layout: detail_layout,
        status: detail_status.clone(),
        reason: detail_reason,
        records: record_chunks,
        nodes: node_chunks,
        finalize: finalize_chunk,
    };

    let mut manifest = TraceManifest {
        trace_schema_version: TRACE_SCHEMA_VERSION,
        trace_id: trace_id.clone(),
        timestamp: Some(timestamp),
        status,
        rule,
        input_format,
        summary,
        max_chunk_bytes_uncompressed: Some(options.max_chunk_bytes_uncompressed as u64),
        detail: Some(detail.clone()),
        masking,
        rule_source,
    };

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

    let manifest_path = trace_dir.join("trace.json");
    let mut manifest_payload = serde_json::to_string_pretty(&manifest)?;
    if manifest_payload.len() as u64 > TRACE_JSON_MAX_BYTES {
        if manifest.rule_source.is_some() {
            manifest.rule_source = None;
            if !detail
                .reason
                .iter()
                .any(|reason| reason == "rule_source_dropped")
            {
                detail.reason.push("rule_source_dropped".to_string());
            }
            manifest.detail = Some(detail.clone());
            manifest_payload = serde_json::to_string_pretty(&manifest)?;
        }
        if manifest_payload.len() as u64 > TRACE_JSON_MAX_BYTES {
            return Err(anyhow::anyhow!(
                "trace json exceeds max bytes: {} > {}",
                manifest_payload.len(),
                TRACE_JSON_MAX_BYTES
            ));
        }
    }
    write_atomic(&manifest_path, manifest_payload.as_bytes())?;
    trace_dir_guard.commit();

    Ok(manifest_path)
}

struct TraceDirGuard {
    path: PathBuf,
    committed: bool,
}

impl TraceDirGuard {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for TraceDirGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn cleanup_detail_files(
    record_files: &mut Vec<PathBuf>,
    node_files: &mut Vec<PathBuf>,
    finalize_file: &mut Option<PathBuf>,
    blob_files: &mut Vec<PathBuf>,
) {
    for path in record_files.iter() {
        if let Err(err) = fs::remove_file(path) {
            warn!(
                "failed to remove record chunk file {}: {}",
                path.display(),
                err
            );
        }
    }
    for path in node_files.iter() {
        if let Err(err) = fs::remove_file(path) {
            warn!(
                "failed to remove node chunk file {}: {}",
                path.display(),
                err
            );
        }
    }
    if let Some(path) = finalize_file.as_ref() {
        if let Err(err) = fs::remove_file(path) {
            warn!(
                "failed to remove finalize chunk file {}: {}",
                path.display(),
                err
            );
        }
    }
    for path in blob_files.iter() {
        if let Err(err) = fs::remove_file(path) {
            warn!("failed to remove blob file {}: {}", path.display(), err);
        }
    }
    record_files.clear();
    node_files.clear();
    *finalize_file = None;
    blob_files.clear();
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

fn reserve_budget(remaining: &mut u64, bytes: u64) -> bool {
    if bytes == 0 {
        return true;
    }
    if *remaining < bytes {
        return false;
    }
    *remaining -= bytes;
    true
}

fn ensure_unique_trace_dir(
    base_dir: &Path,
    trace_id: String,
    raw_trace_id: &str,
) -> Result<(String, PathBuf)> {
    fs::create_dir_all(base_dir)?;
    let base_id = trace_id;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut counter = 0usize;
    loop {
        let candidate = if counter == 0 {
            base_id.clone()
        } else {
            let suffix = if counter == 1 {
                format!("dup-{nanos}")
            } else {
                format!("dup-{nanos}-{}", counter - 1)
            };
            format!("{base_id}-{suffix}")
        };
        let trace_dir = base_dir.join(&candidate);
        match fs::create_dir(&trace_dir) {
            Ok(()) => {
                if counter > 0 {
                    warn!(
                        "trace_id collision for {}; using {} instead",
                        raw_trace_id, candidate
                    );
                }
                return Ok((candidate, trace_dir));
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                counter = counter.saturating_add(1);
                continue;
            }
            Err(err) => return Err(err.into()),
        }
    }
}

fn count_inline_nodes(records: &[JsonValue], max_nodes: usize) -> usize {
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

fn blob_total_bytes(blob_files: &[PathBuf]) -> u64 {
    blob_files
        .iter()
        .map(|path| match fs::metadata(path) {
            Ok(meta) => meta.len(),
            Err(err) => {
                warn!("failed to read blob metadata {}: {}", path.display(), err);
                0
            }
        })
        .sum()
}
