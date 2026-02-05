use std::collections::{HashSet, VecDeque};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use chrono::{Datelike, Utc};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::trace_backend::TraceWriteBackend;
use crate::trace_id::{sanitize_trace_id, trace_id_is_placeholder};
use crate::trace_schema::{
    RuleMeta, TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TRACE_CHUNK_COUNT_HARD_MAX,
    TRACE_JSON_MAX_BYTES, TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX, TraceChunkRef,
    TraceDetailRef, TraceManifest, TraceMasking, TraceSummary,
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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum TracePriority {
    High,
    Normal,
}

struct TraceWriteRequest {
    trace: JsonValue,
    options: TraceWriteOptions,
    priority: TracePriority,
    downgraded: bool,
    approx_bytes: usize,
}

struct TraceQueue {
    backend: Arc<dyn TraceWriteBackend>,
    capacity: usize,
    max_bytes: usize,
    items: Mutex<VecDeque<TraceWriteRequest>>,
    cvar: Condvar,
}

impl TraceQueue {
    fn new(backend: Arc<dyn TraceWriteBackend>, capacity: usize, max_bytes: usize) -> Self {
        Self {
            backend,
            capacity: capacity.max(1),
            max_bytes: max_bytes.max(1),
            items: Mutex::new(VecDeque::new()),
            cvar: Condvar::new(),
        }
    }
}

fn trace_writer_loop(queue: Arc<TraceQueue>) {
    loop {
        let request = {
            let mut guard = queue.items.lock().expect("trace queue lock");
            while guard.is_empty() {
                guard = queue.cvar.wait(guard).expect("trace queue wait");
            }
            guard.pop_front()
        };
        let Some(request) = request else {
            continue;
        };
        if request.downgraded {
            warn!(
                "trace queue full; downgraded trace {} to basic",
                trace_id_for_log(&request.trace)
            );
        }
        if let Err(err) = queue
            .backend
            .write_trace_bundle(&request.trace, &request.options)
        {
            warn!("failed to write trace bundle: {}", err);
        }
    }
}

fn push_request(queue: &mut VecDeque<TraceWriteRequest>, request: TraceWriteRequest) {
    match request.priority {
        TracePriority::High => queue.push_front(request),
        TracePriority::Normal => queue.push_back(request),
    }
}

fn evict_normal(queue: &mut VecDeque<TraceWriteRequest>) -> bool {
    let position = queue
        .iter()
        .position(|item| item.priority == TracePriority::Normal);
    if let Some(index) = position {
        queue.remove(index);
        return true;
    }
    false
}

fn trace_id_for_log(trace: &JsonValue) -> String {
    trace
        .get("trace_id")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
        .to_string()
}

fn estimate_trace_bytes(trace: &JsonValue) -> usize {
    struct CountingWriter {
        size: usize,
    }

    impl Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.size = self.size.saturating_add(buf.len());
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut writer = CountingWriter { size: 0 };
    if serde_json::to_writer(&mut writer, trace).is_ok() {
        writer.size
    } else {
        0
    }
}

fn queue_bytes(queue: &VecDeque<TraceWriteRequest>) -> usize {
    queue
        .iter()
        .map(|request| request.approx_bytes)
        .sum::<usize>()
}

fn strip_trace_detail(trace: &mut JsonValue) {
    let Some(obj) = trace.as_object_mut() else {
        return;
    };
    obj.remove("records");
    obj.remove("finalize");
    obj.remove("nodes");
}

#[cfg(test)]
mod queue_tests {
    use super::*;
    use serde_json::json;

    fn make_trace(trace_id: &str, status: &str) -> JsonValue {
        json!({
            "trace_id": trace_id,
            "status": status,
            "records": [
                { "index": 0, "status": status }
            ],
            "summary": {
                "record_total": 1,
                "record_success": if status == "ok" { 1 } else { 0 },
                "record_failed": if status == "ok" { 0 } else { 1 }
            }
        })
    }

    #[test]
    fn queue_full_normal_drops_new_trace() {
        let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-normal-drop");
        let writer = TraceWriter::with_config(
            temp_dir,
            TraceWriterConfig {
                queue_capacity: 1,
                queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
                write_options: TraceWriteOptions::default(),
                spawn_worker: false,
                write_backend: None,
            },
        );

        assert!(writer.enqueue(make_trace("trace-a", "ok")));
        assert!(!writer.enqueue(make_trace("trace-b", "ok")));

        let guard = writer.queue.items.lock().expect("queue lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(trace_id_for_log(&guard[0].trace), "trace-a");
        assert_eq!(guard[0].priority, TracePriority::Normal);
    }

    #[test]
    fn queue_full_high_evicts_normal_and_keeps_full() {
        let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-high-evict");
        let writer = TraceWriter::with_config(
            temp_dir,
            TraceWriterConfig {
                queue_capacity: 1,
                queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
                write_options: TraceWriteOptions::default(),
                spawn_worker: false,
                write_backend: None,
            },
        );

        assert!(writer.enqueue(make_trace("trace-normal", "ok")));

        let options = TraceWriteOptions {
            detail_level: TraceDetailLevel::Full,
            ..Default::default()
        };
        assert!(writer.enqueue_with_options(make_trace("trace-error", "error"), Some(options)));

        let guard = writer.queue.items.lock().expect("queue lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(trace_id_for_log(&guard[0].trace), "trace-error");
        assert_eq!(guard[0].priority, TracePriority::High);
        assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Full);
        assert!(!guard[0].downgraded);
    }

    #[test]
    fn queue_max_bytes_downgrades_and_strips_trace() {
        let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-max-bytes");
        let writer = TraceWriter::with_config(
            temp_dir,
            TraceWriterConfig {
                queue_capacity: 10,
                queue_max_bytes: 512,
                write_options: TraceWriteOptions::default(),
                spawn_worker: false,
                write_backend: None,
            },
        );

        let trace = json!({
            "trace_id": "trace-big",
            "status": "ok",
            "records": [
                { "index": 0, "payload": "x".repeat(1024) }
            ],
            "summary": {
                "record_total": 1,
                "record_success": 1,
                "record_failed": 0
            }
        });

        assert!(writer.enqueue(trace));

        let guard = writer.queue.items.lock().expect("queue lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Basic);
        assert!(guard[0].downgraded);
        assert!(guard[0].trace.get("records").is_none());
    }

    #[test]
    fn queue_full_downgrade_sets_reason() {
        let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-reason");
        let writer = TraceWriter::with_config(
            temp_dir,
            TraceWriterConfig {
                queue_capacity: 2,
                queue_max_bytes: 256,
                write_options: TraceWriteOptions::default(),
                spawn_worker: false,
                write_backend: None,
            },
        );

        let trace = json!({
            "trace_id": "trace-queue-reason",
            "status": "ok",
            "records": [
                { "index": 0, "status": "ok", "payload": "x".repeat(2048) }
            ],
            "summary": {
                "record_total": 1,
                "record_success": 1,
                "record_failed": 0
            }
        });

        assert!(writer.enqueue(trace));

        let guard = writer.queue.items.lock().expect("queue lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(
            guard[0].options.detail_reason.as_deref(),
            Some("queue_full")
        );
        assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Basic);
    }

    #[test]
    fn queue_full_downgrade_appends_reason() {
        let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-reason-append");
        let writer = TraceWriter::with_config(
            temp_dir,
            TraceWriterConfig {
                queue_capacity: 2,
                queue_max_bytes: 256,
                write_options: TraceWriteOptions::default(),
                spawn_worker: false,
                write_backend: None,
            },
        );

        let trace = json!({
            "trace_id": "trace-queue-reason-append",
            "status": "ok",
            "records": [
                { "index": 0, "status": "ok", "payload": "x".repeat(2048) }
            ],
            "summary": {
                "record_total": 1,
                "record_success": 1,
                "record_failed": 0
            }
        });

        let options = TraceWriteOptions {
            detail_reason: Some("sampled_out".to_string()),
            ..TraceWriteOptions::default()
        };

        assert!(writer.enqueue_with_options(trace, Some(options)));

        let guard = writer.queue.items.lock().expect("queue lock");
        assert_eq!(guard.len(), 1);
        let reason = guard[0]
            .options
            .detail_reason
            .as_deref()
            .expect("detail reason");
        assert!(reason.contains("sampled_out"));
        assert!(reason.contains("queue_full"));
        assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Basic);
    }

    #[test]
    fn queue_max_bytes_high_priority_downgrades_when_needed() {
        let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-high-max-bytes");
        let writer = TraceWriter::with_config(
            temp_dir,
            TraceWriterConfig {
                queue_capacity: 10,
                queue_max_bytes: 512,
                write_options: TraceWriteOptions::default(),
                spawn_worker: false,
                write_backend: None,
            },
        );

        let trace = json!({
            "trace_id": "trace-error-big",
            "status": "error",
            "records": [
                { "index": 0, "payload": "x".repeat(1024) }
            ],
            "summary": {
                "record_total": 1,
                "record_success": 0,
                "record_failed": 1
            }
        });

        let options = TraceWriteOptions {
            detail_level: TraceDetailLevel::Full,
            ..Default::default()
        };
        assert!(writer.enqueue_with_options(trace, Some(options)));

        let guard = writer.queue.items.lock().expect("queue lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].priority, TracePriority::High);
        assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Basic);
        assert!(guard[0].downgraded);
        assert!(guard[0].trace.get("records").is_none());
    }

    #[test]
    fn enqueue_basic_strips_and_masks_rule_source() {
        let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-basic-mask");
        let writer = TraceWriter::with_config(
            temp_dir,
            TraceWriterConfig {
                queue_capacity: DEFAULT_TRACE_QUEUE_CAPACITY,
                queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
                write_options: TraceWriteOptions::default(),
                spawn_worker: false,
                write_backend: None,
            },
        );

        let trace = json!({
            "trace_id": "trace-basic-mask",
            "status": "ok",
            "records": [
                { "index": 0, "input": { "token": "secret" } }
            ],
            "rule_source": {
                "token": "secret",
                "headers": {
                    "Authorization": "Bearer 123"
                }
            }
        });

        let options = TraceWriteOptions {
            detail_level: TraceDetailLevel::Basic,
            ..Default::default()
        };

        assert!(writer.enqueue_with_options(trace, Some(options)));

        let guard = writer.queue.items.lock().expect("queue lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Basic);
        assert!(guard[0].trace.get("records").is_none());
        let rule_source = guard[0]
            .trace
            .get("rule_source")
            .and_then(|value| value.as_object())
            .expect("rule_source should exist");
        assert_eq!(
            rule_source.get("token").and_then(|value| value.as_str()),
            Some("[masked]")
        );
        let headers = rule_source
            .get("headers")
            .and_then(|value| value.as_object())
            .expect("headers should exist");
        assert_eq!(
            headers
                .get("Authorization")
                .and_then(|value| value.as_str()),
            Some("[masked]")
        );
    }

    #[test]
    fn enqueue_basic_masks_summary_fields() {
        let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-basic-summary-mask");
        let writer = TraceWriter::with_config(
            temp_dir,
            TraceWriterConfig {
                queue_capacity: DEFAULT_TRACE_QUEUE_CAPACITY,
                queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
                write_options: TraceWriteOptions::default(),
                spawn_worker: false,
                write_backend: None,
            },
        );

        let trace = json!({
            "trace_id": "trace-basic-summary-mask",
            "status": "ok",
            "records": [
                { "index": 0, "input": { "token": "secret" } }
            ],
            "summary": {
                "input": { "password": "secret", "ok": 1 },
                "output": { "token": "secret" }
            }
        });

        let options = TraceWriteOptions {
            detail_level: TraceDetailLevel::Basic,
            ..Default::default()
        };

        assert!(writer.enqueue_with_options(trace, Some(options)));

        let guard = writer.queue.items.lock().expect("queue lock");
        assert_eq!(guard.len(), 1);
        assert!(guard[0].trace.get("records").is_none());
        let summary = guard[0]
            .trace
            .get("summary")
            .and_then(|value| value.as_object())
            .expect("summary should exist");
        let summary_input = summary
            .get("input")
            .and_then(|value| value.as_object())
            .expect("summary input");
        let summary_output = summary
            .get("output")
            .and_then(|value| value.as_object())
            .expect("summary output");
        assert_eq!(
            summary_input
                .get("password")
                .and_then(|value| value.as_str()),
            Some("[masked]")
        );
        assert_eq!(
            summary_output.get("token").and_then(|value| value.as_str()),
            Some("[masked]")
        );
    }

    #[test]
    fn enqueue_off_masks_summary_fields() {
        let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-off-summary-mask");
        let writer = TraceWriter::with_config(
            temp_dir,
            TraceWriterConfig {
                queue_capacity: DEFAULT_TRACE_QUEUE_CAPACITY,
                queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
                write_options: TraceWriteOptions::default(),
                spawn_worker: false,
                write_backend: None,
            },
        );

        let trace = json!({
            "trace_id": "trace-off-summary-mask",
            "status": "ok",
            "records": [
                { "index": 0, "input": { "token": "secret" } }
            ],
            "summary": {
                "input": { "password": "secret", "ok": 1 },
                "output": { "token": "secret" }
            },
            "rule_source": {
                "token": "secret"
            }
        });

        let options = TraceWriteOptions {
            detail_level: TraceDetailLevel::Off,
            ..Default::default()
        };

        assert!(writer.enqueue_with_options(trace, Some(options)));

        let guard = writer.queue.items.lock().expect("queue lock");
        assert_eq!(guard.len(), 1);
        assert!(guard[0].trace.get("records").is_none());
        let summary = guard[0]
            .trace
            .get("summary")
            .and_then(|value| value.as_object())
            .expect("summary should exist");
        let summary_input = summary
            .get("input")
            .and_then(|value| value.as_object())
            .expect("summary input");
        let summary_output = summary
            .get("output")
            .and_then(|value| value.as_object())
            .expect("summary output");
        assert_eq!(
            summary_input
                .get("password")
                .and_then(|value| value.as_str()),
            Some("[masked]")
        );
        assert_eq!(
            summary_output.get("token").and_then(|value| value.as_str()),
            Some("[masked]")
        );
        let rule_source = guard[0]
            .trace
            .get("rule_source")
            .and_then(|value| value.as_object())
            .expect("rule_source should exist");
        assert_eq!(
            rule_source.get("token").and_then(|value| value.as_str()),
            Some("[masked]")
        );
    }
}

#[cfg(test)]
mod write_failure_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn write_trace_bundle_cleans_up_on_manifest_write_failure() {
        let temp_dir = std::env::temp_dir().join("rulemorph-trace-write-failure-cleanup");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).expect("create temp dir");

        let trace = json!({
            "trace_id": "trace-fail-cleanup",
            "records": [
                { "index": 0, "status": "ok", "output": { "value": 1 } }
            ]
        });

        let _guard = fail_write_for_trace_id("trace-fail-cleanup", Some("trace.json"));
        let result = write_trace_bundle_sync(&temp_dir, &trace, &TraceWriteOptions::default());
        assert!(result.is_err());

        let now = Utc::now();
        let trace_dir = temp_dir
            .join("traces")
            .join(format!("{:04}", now.year()))
            .join(format!("{:02}", now.month()))
            .join(format!("{:02}", now.day()))
            .join("trace-fail-cleanup");
        assert!(
            !trace_dir.exists(),
            "trace dir should be removed after failure"
        );
    }
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

fn normalize_inline_records(records: &[JsonValue]) -> Vec<JsonValue> {
    records
        .iter()
        .map(|record| {
            let mut record_clone = record.clone();
            if let Some(obj) = record_clone.as_object_mut() {
                if let Some(nodes_value) = obj.get("nodes").cloned() {
                    let normalized = normalize_nodes_value(&nodes_value);
                    obj.insert("nodes".to_string(), JsonValue::Array(normalized));
                }
            }
            record_clone
        })
        .collect()
}

fn normalize_nodes_value(nodes_value: &JsonValue) -> Vec<JsonValue> {
    match nodes_value {
        JsonValue::Array(nodes) => nodes.iter().map(normalize_node_value).collect(),
        JsonValue::Object(_) => vec![normalize_node_value(nodes_value)],
        other => vec![json!({ "value": other })],
    }
}

fn normalize_node_value(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => JsonValue::Object(map.clone()),
        other => json!({ "value": other }),
    }
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

fn normalize_masking_rules(rules: &[String]) -> Vec<String> {
    if rules.is_empty() {
        return Vec::new();
    }
    rules
        .iter()
        .map(|rule| rule.trim().to_ascii_lowercase())
        .filter(|rule| !rule.is_empty())
        .collect()
}

fn mask_url_query(value: &str, rules: &[String]) -> Option<String> {
    if rules.is_empty() || !value.contains('?') {
        return None;
    }
    let (base, rest) = value.split_once('?')?;
    let (query, fragment) = match rest.split_once('#') {
        Some((query, fragment)) => (query, Some(fragment)),
        None => (rest, None),
    };
    let mut masked = false;
    let mut parts = Vec::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            parts.push(String::new());
            continue;
        }
        if let Some((key, _value)) = pair.split_once('=') {
            if should_mask_key(key, rules) {
                masked = true;
                parts.push(format!("{key}=[masked]"));
            } else {
                parts.push(pair.to_string());
            }
        } else if should_mask_key(pair, rules) {
            masked = true;
            parts.push(format!("{pair}=[masked]"));
        } else {
            parts.push(pair.to_string());
        }
    }
    if !masked {
        return None;
    }
    let mut masked_value = String::with_capacity(value.len());
    masked_value.push_str(base);
    masked_value.push('?');
    masked_value.push_str(&parts.join("&"));
    if let Some(fragment) = fragment {
        masked_value.push('#');
        masked_value.push_str(fragment);
    }
    Some(masked_value)
}

fn apply_masking(value: &mut JsonValue, rules: &[String]) {
    match value {
        JsonValue::Object(map) => {
            for (key, entry) in map.iter_mut() {
                if should_mask_key(key, rules) {
                    *entry = JsonValue::String("[masked]".to_string());
                } else {
                    apply_masking(entry, rules);
                }
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                apply_masking(item, rules);
            }
        }
        JsonValue::String(value) => {
            if let Some(masked) = mask_url_query(value, rules) {
                *value = masked;
            }
        }
        _ => {}
    }
}

fn should_mask_key(key: &str, rules: &[String]) -> bool {
    if rules.is_empty() {
        return false;
    }
    let key_lower = key.to_ascii_lowercase();
    rules.iter().any(|rule| key_lower.contains(rule))
}

fn externalize_trace_payloads(
    trace: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    if options.max_payload_bytes == 0 {
        return Ok(false);
    }
    let mut seen = HashSet::new();
    externalize_trace_payloads_inner(
        trace,
        trace_dir,
        options,
        &mut seen,
        budget_remaining,
        blob_files,
    )
}

fn externalize_trace_payloads_inner(
    trace: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    let Some(obj) = trace.as_object_mut() else {
        return Ok(false);
    };
    if let Some(records) = obj
        .get_mut("records")
        .and_then(|value| value.as_array_mut())
    {
        for record in records {
            if externalize_record_payloads(
                record,
                trace_dir,
                options,
                seen,
                budget_remaining,
                blob_files,
            )? {
                return Ok(true);
            }
        }
    }
    if let Some(finalize) = obj.get_mut("finalize") {
        if externalize_finalize_payloads(
            finalize,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn externalize_record_payloads(
    record: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    let Some(obj) = record.as_object_mut() else {
        return Ok(false);
    };
    if let Some(input) = obj.get_mut("input") {
        if maybe_externalize_payload(
            input,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(output) = obj.get_mut("output") {
        if maybe_externalize_payload(
            output,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(nodes_value) = obj.get_mut("nodes") {
        match nodes_value {
            JsonValue::Array(nodes) => {
                for node in nodes {
                    if externalize_node_payloads(
                        node,
                        trace_dir,
                        options,
                        seen,
                        budget_remaining,
                        blob_files,
                    )? {
                        return Ok(true);
                    }
                }
            }
            JsonValue::Object(_) => {
                if externalize_node_payloads(
                    nodes_value,
                    trace_dir,
                    options,
                    seen,
                    budget_remaining,
                    blob_files,
                )? {
                    return Ok(true);
                }
            }
            _ => {}
        }
    }
    if let Some(child_trace) = obj.get_mut("child_trace") {
        if externalize_trace_payloads_inner(
            child_trace,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn externalize_node_payloads(
    node: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    let Some(obj) = node.as_object_mut() else {
        return Ok(false);
    };
    if let Some(input) = obj.get_mut("input") {
        if maybe_externalize_payload(
            input,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(output) = obj.get_mut("output") {
        if maybe_externalize_payload(
            output,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(args) = obj.get_mut("args") {
        if maybe_externalize_payload(args, trace_dir, options, seen, budget_remaining, blob_files)?
        {
            return Ok(true);
        }
    }
    if let Some(pipe_value) = obj.get_mut("pipe_value") {
        if maybe_externalize_payload(
            pipe_value,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(pipe_steps) = obj
        .get_mut("pipe_steps")
        .and_then(|value| value.as_array_mut())
    {
        for step in pipe_steps {
            if let Some(step_obj) = step.as_object_mut() {
                if let Some(input) = step_obj.get_mut("input") {
                    if maybe_externalize_payload(
                        input,
                        trace_dir,
                        options,
                        seen,
                        budget_remaining,
                        blob_files,
                    )? {
                        return Ok(true);
                    }
                }
                if let Some(output) = step_obj.get_mut("output") {
                    if maybe_externalize_payload(
                        output,
                        trace_dir,
                        options,
                        seen,
                        budget_remaining,
                        blob_files,
                    )? {
                        return Ok(true);
                    }
                }
            }
        }
    }
    if let Some(children) = obj
        .get_mut("children")
        .and_then(|value| value.as_array_mut())
    {
        for child in children {
            if externalize_node_payloads(
                child,
                trace_dir,
                options,
                seen,
                budget_remaining,
                blob_files,
            )? {
                return Ok(true);
            }
        }
    }
    if let Some(child_trace) = obj.get_mut("child_trace") {
        if externalize_trace_payloads_inner(
            child_trace,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn externalize_finalize_payloads(
    finalize: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    let Some(obj) = finalize.as_object_mut() else {
        return Ok(false);
    };
    if let Some(input) = obj.get_mut("input") {
        if maybe_externalize_payload(
            input,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(output) = obj.get_mut("output") {
        if maybe_externalize_payload(
            output,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(nodes) = obj.get_mut("nodes").and_then(|value| value.as_array_mut()) {
        for node in nodes {
            if externalize_node_payloads(
                node,
                trace_dir,
                options,
                seen,
                budget_remaining,
                blob_files,
            )? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn maybe_externalize_payload(
    value: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    if value.is_null() || is_externalized_payload(value) {
        return Ok(false);
    }
    let raw = serde_json::to_vec(value)?;
    if raw.len() <= options.max_payload_bytes {
        return Ok(false);
    }
    let preview_limit = options.payload_preview_bytes.min(options.max_payload_bytes);
    let preview = build_payload_preview(&raw, preview_limit);
    match write_blob(trace_dir, &raw, options, seen, budget_remaining)? {
        WriteBlobResult::Written {
            blob_ref,
            blob_path,
        } => {
            if !blob_files.contains(&blob_path) {
                blob_files.push(blob_path);
            }
            *value = json!({
                "preview": preview,
                "size_bytes": raw.len() as u64,
                "blob_ref": blob_ref
            });
            Ok(false)
        }
        WriteBlobResult::BudgetExceeded => Ok(true),
    }
}

fn is_externalized_payload(value: &JsonValue) -> bool {
    value.get("blob_ref").is_some()
        && value.get("size_bytes").is_some()
        && value.get("preview").is_some()
}

fn build_payload_preview(raw: &[u8], limit: usize) -> String {
    if limit == 0 || raw.is_empty() {
        return String::new();
    }
    if raw.len() <= limit {
        return String::from_utf8_lossy(raw).into_owned();
    }
    let mut preview = String::from_utf8_lossy(&raw[..limit]).into_owned();
    preview.push_str("...");
    preview
}

enum WriteBlobResult {
    Written {
        blob_ref: String,
        blob_path: PathBuf,
    },
    BudgetExceeded,
}

fn write_blob(
    trace_dir: &Path,
    raw: &[u8],
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
) -> Result<WriteBlobResult> {
    let hash = Sha256::digest(raw);
    let hash_hex = hex_encode(&hash);
    let extension = match options.compression {
        TraceCompression::Zstd => ".zst",
        TraceCompression::None => "",
    };
    let filename = format!("sha256-{hash_hex}.json{extension}");
    let rel_path = PathBuf::from("blobs").join(filename);
    let rel_string = rel_path.to_string_lossy().to_string();
    let full_path = trace_dir.join(&rel_path);
    if !seen.insert(rel_string.clone()) {
        return Ok(WriteBlobResult::Written {
            blob_ref: rel_string,
            blob_path: full_path,
        });
    }
    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file_exists = full_path.exists();
    let mut needs_write = !file_exists;
    let mut payload: Option<Vec<u8>> = None;
    let bytes = if file_exists {
        match fs::metadata(&full_path) {
            Ok(metadata) => metadata.len(),
            Err(_) => {
                let computed = match options.compression {
                    TraceCompression::Zstd => zstd::stream::encode_all(raw, 3)?,
                    TraceCompression::None => raw.to_vec(),
                };
                let bytes = computed.len() as u64;
                payload = Some(computed);
                needs_write = true;
                bytes
            }
        }
    } else {
        let computed = match options.compression {
            TraceCompression::Zstd => zstd::stream::encode_all(raw, 3)?,
            TraceCompression::None => raw.to_vec(),
        };
        let bytes = computed.len() as u64;
        payload = Some(computed);
        bytes
    };
    if !reserve_budget(budget_remaining, bytes) {
        return Ok(WriteBlobResult::BudgetExceeded);
    }
    if needs_write {
        if let Some(payload) = payload {
            write_atomic(&full_path, payload.as_slice())?;
        }
    }
    Ok(WriteBlobResult::Written {
        blob_ref: rel_string,
        blob_path: full_path,
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{:02x}", byte));
    }
    output
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

fn trace_priority(trace: &JsonValue, options: &TraceWriteOptions) -> TracePriority {
    let records = trace
        .get("records")
        .and_then(|value| value.as_array())
        .map(|value| value.as_slice())
        .unwrap_or(&[]);
    if trace_is_error(trace, records) || trace_is_slow(trace, records, options) {
        TracePriority::High
    } else {
        TracePriority::Normal
    }
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

fn parse_record_index(value: &JsonValue) -> Option<u64> {
    match value {
        JsonValue::Number(num) => num.as_u64(),
        JsonValue::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn split_records_and_nodes(records: &[JsonValue]) -> (Vec<JsonValue>, Vec<JsonValue>) {
    let mut records_out = Vec::with_capacity(records.len());
    let mut nodes_out = Vec::new();
    let mut seen_indices: HashSet<u64> = HashSet::new();

    for (index, record) in records.iter().enumerate() {
        let mut record_index = record
            .get("index")
            .and_then(parse_record_index)
            .unwrap_or(index as u64);
        if seen_indices.contains(&record_index) {
            record_index = index as u64;
        }
        if seen_indices.contains(&record_index) {
            let start = record_index;
            loop {
                record_index = record_index.wrapping_add(1);
                if !seen_indices.contains(&record_index) {
                    break;
                }
                if record_index == start {
                    warn!(
                        "record_index space exhausted while deduplicating; using {}",
                        record_index
                    );
                    break;
                }
            }
        }
        seen_indices.insert(record_index);
        if let Some(nodes_value) = record.get("nodes") {
            match nodes_value {
                JsonValue::Array(nodes) => {
                    for node in nodes {
                        let mut entry = match node {
                            JsonValue::Object(map) => JsonValue::Object(map.clone()),
                            other => json!({ "value": other }),
                        };
                        if let Some(obj) = entry.as_object_mut() {
                            obj.insert("record_index".to_string(), JsonValue::from(record_index));
                        }
                        nodes_out.push(entry);
                    }
                }
                other => {
                    let mut entry = match other {
                        JsonValue::Object(map) => JsonValue::Object(map.clone()),
                        value => json!({ "value": value }),
                    };
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert("record_index".to_string(), JsonValue::from(record_index));
                    }
                    nodes_out.push(entry);
                }
            }
        }

        let mut record_clone = record.clone();
        if let Some(obj) = record_clone.as_object_mut() {
            obj.insert("index".to_string(), JsonValue::from(record_index));
            if obj.contains_key("nodes") {
                obj.remove("nodes");
            }
        }
        records_out.push(record_clone);
    }

    (records_out, nodes_out)
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

struct ChunkWriteResult {
    chunks: Vec<TraceChunkRef>,
    files: Vec<PathBuf>,
    budget_exceeded: bool,
}

struct FinalizeWriteResult {
    chunk: Option<TraceChunkRef>,
    file: Option<PathBuf>,
    budget_exceeded: bool,
    size_exceeded: bool,
}

fn write_record_chunks(
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

fn write_node_chunks(
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

fn max_ndjson_line_bytes(items: &[JsonValue], label: &str) -> Result<usize> {
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

fn total_chunk_bytes(chunks: &[TraceChunkRef]) -> u64 {
    chunks.iter().filter_map(|chunk| chunk.bytes).sum()
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

fn write_finalize_chunk(
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

fn write_atomic(path: &Path, payload: &[u8]) -> Result<()> {
    #[cfg(test)]
    if should_fail_write(path) {
        return Err(anyhow::anyhow!("forced write failure"));
    }
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

#[cfg(test)]
fn should_fail_write(path: &Path) -> bool {
    let config = fail_write_config()
        .lock()
        .expect("fail write config lock")
        .clone();
    let Some(config) = config else {
        return false;
    };
    if !path
        .components()
        .any(|component| component.as_os_str() == config.trace_id)
    {
        return false;
    }
    if let Some(filename) = config.filename.as_ref() {
        return path.file_name() == Some(filename);
    }
    true
}

#[cfg(test)]
#[derive(Clone, Debug)]
struct FailWriteConfig {
    trace_id: std::ffi::OsString,
    filename: Option<std::ffi::OsString>,
}

#[cfg(test)]
fn fail_write_config() -> &'static Mutex<Option<FailWriteConfig>> {
    static FAIL_WRITE_CONFIG: OnceLock<Mutex<Option<FailWriteConfig>>> = OnceLock::new();
    FAIL_WRITE_CONFIG.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
struct FailWriteGuard;

#[cfg(test)]
impl Drop for FailWriteGuard {
    fn drop(&mut self) {
        let mut guard = fail_write_config().lock().expect("fail write config lock");
        *guard = None;
    }
}

#[cfg(test)]
fn fail_write_for_trace_id(trace_id: &str, filename: Option<&str>) -> FailWriteGuard {
    let mut guard = fail_write_config().lock().expect("fail write config lock");
    *guard = Some(FailWriteConfig {
        trace_id: std::ffi::OsString::from(trace_id),
        filename: filename.map(std::ffi::OsString::from),
    });
    FailWriteGuard
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
