use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::trace_schema::{RuleMeta, TraceManifest, TraceSummary};

mod chunk_access;
mod chunk_read;
mod file_backend;
mod import_bundle;
mod import_path;
mod index;
mod legacy;
mod manifest_budget;
mod manifest_trace;
mod meta;
mod purge;
mod store;
#[cfg(test)]
mod tests;
mod trace_id_path;

#[cfg(test)]
use self::chunk_read::ChunkBudget;
#[cfg(test)]
use self::chunk_read::resolve_chunk_path;
pub use self::file_backend::FileTraceBackend;
use self::index::is_trace_meta_candidate;
#[cfg(test)]
use self::legacy::apply_legacy_limits_with_thresholds;
#[cfg(test)]
use self::manifest_trace::build_trace_from_manifest_with_budget;
#[cfg(test)]
use self::purge::resolve_trace_timestamp;
pub use self::store::TraceStore;
#[cfg(test)]
use self::trace_id_path::{fallback_trace_id_for_path, path_hash_for_trace_id};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceMeta {
    pub trace_id: String,
    pub status: String,
    pub timestamp: Option<String>,
    pub duration_us: Option<u64>,
    pub rule: Option<RuleMeta>,
    pub summary: Option<TraceSummary>,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceNodeChunkEntry {
    pub record_index: u64,
    pub node: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportResult {
    pub imported: usize,
    pub trace_ids: Vec<String>,
    pub rules_imported: usize,
}

#[derive(Debug, Clone)]
pub struct PurgeFailure {
    pub trace_id: String,
    pub path: String,
    pub error: String,
}

#[derive(Debug, Clone)]
pub struct PurgeReport {
    pub purged: Vec<TraceMeta>,
    pub failed: Vec<PurgeFailure>,
}

fn traces_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("traces")
}

fn rules_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("rules")
}

// copy_dir_recursive was intentionally omitted to avoid counting existing files.
