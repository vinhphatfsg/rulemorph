use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX: usize = 16 * 1024 * 1024;
pub const TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX: usize = 1024 * 1024;
pub const TRACE_CHUNK_BYTES_COMPRESSED_HARD_MAX: usize =
    TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX + TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX;
pub const TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX: usize =
    TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX * 16;
pub const TRACE_CHUNK_COUNT_HARD_MAX: usize = 128;
pub const TRACE_JSON_MAX_BYTES: u64 = 20 * 1024 * 1024;
pub const TRACE_RECORD_COUNT_HARD_MAX: usize = 200_000;
pub const TRACE_NODE_COUNT_HARD_MAX: usize = 500_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleMeta {
    pub name: Option<String>,
    pub path: Option<String>,
    pub r#type: Option<String>,
    pub version: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSummary {
    pub record_total: Option<u64>,
    pub record_success: Option<u64>,
    pub record_failed: Option<u64>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub duration_us: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceChunkRef {
    pub path: String,
    pub format: String,
    pub compression: String,
    #[serde(default)]
    pub record_start: Option<u64>,
    #[serde(default)]
    pub record_end: Option<u64>,
    #[serde(default)]
    pub node_start: Option<u64>,
    #[serde(default)]
    pub node_end: Option<u64>,
    #[serde(default)]
    pub bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceDetailRef {
    pub layout: String,
    pub status: String,
    #[serde(default)]
    pub reason: Vec<String>,
    #[serde(default)]
    pub records: Vec<TraceChunkRef>,
    #[serde(default)]
    pub nodes: Vec<TraceChunkRef>,
    #[serde(default)]
    pub finalize: Option<TraceChunkRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceMasking {
    pub enabled: bool,
    #[serde(default)]
    pub rules: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceManifest {
    pub trace_schema_version: u8,
    pub trace_id: String,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub rule: Option<RuleMeta>,
    #[serde(default)]
    pub input_format: Option<String>,
    #[serde(default)]
    pub summary: Option<TraceSummary>,
    #[serde(default)]
    pub max_chunk_bytes_uncompressed: Option<u64>,
    #[serde(default)]
    pub detail: Option<TraceDetailRef>,
    #[serde(default)]
    pub masking: Option<TraceMasking>,
    #[serde(default)]
    pub rule_source: Option<JsonValue>,
}
