use std::sync::Arc;

use crate::trace_backend::TraceWriteBackend;
use crate::trace_schema::TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX;

const DEFAULT_MAX_RECORDS_PER_CHUNK: usize = 200;
const DEFAULT_MAX_NODES_PER_CHUNK: usize = 2000;
const DEFAULT_MAX_CHUNK_BYTES: usize = 4 * 1024 * 1024; // 4MB
const DEFAULT_MAX_TRACE_BYTES: usize = 10 * 1024 * 1024; // 10MB (compressed)
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 64 * 1024;
const DEFAULT_PAYLOAD_PREVIEW_BYTES: usize = 1024;
pub(super) const DEFAULT_SAMPLING_RATE: f64 = 1.0;
pub(super) const DEFAULT_TRACE_QUEUE_CAPACITY: usize = 256;
pub(super) const DEFAULT_TRACE_QUEUE_MAX_BYTES: usize = 64 * 1024 * 1024;

pub(super) fn clamp_max_chunk_bytes_uncompressed(value: usize) -> usize {
    value.clamp(1, TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX)
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
