use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::warn;

use crate::trace_backend::TraceBackend;
use crate::trace_schema::{RuleMeta, TraceManifest, TraceSummary};

mod chunk_access;
mod chunk_read;
mod import_bundle;
mod import_path;
mod index;
mod legacy;
mod manifest_budget;
mod manifest_trace;
mod meta;
mod purge;
#[cfg(test)]
mod tests;
mod trace_id_path;

use self::chunk_access::{
    get_finalize_chunk as get_manifest_finalize_chunk, get_nodes_chunk as get_manifest_nodes_chunk,
    get_records_chunk as get_manifest_records_chunk,
};
#[cfg(test)]
use self::chunk_read::ChunkBudget;
#[cfg(test)]
use self::chunk_read::resolve_chunk_path;
use self::import_bundle::{IMPORT_MAX_TOTAL_BYTES, import_bundle_files};
use self::index::{build_trace_index, is_trace_meta_candidate};
#[cfg(test)]
use self::legacy::apply_legacy_limits_with_thresholds;
use self::legacy::{apply_legacy_limits, looks_like_legacy_trace};
use self::manifest_budget::{apply_manifest_budget, apply_record_total_budget_for_get};
use self::manifest_trace::build_trace_from_manifest_async;
#[cfg(test)]
use self::manifest_trace::build_trace_from_manifest_with_budget;
use self::meta::{is_manifest, read_trace_json_with_limit_async};
use self::purge::purge_trace_metas;
#[cfg(test)]
use self::purge::resolve_trace_timestamp;
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

#[derive(Debug, Clone)]
pub struct FileTraceBackend {
    data_dir: PathBuf,
    index: Arc<RwLock<HashMap<String, TraceMeta>>>,
}

impl FileTraceBackend {
    pub async fn new(data_dir: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(traces_dir(&data_dir)).await?;
        tokio::fs::create_dir_all(rules_dir(&data_dir)).await?;

        let store = Self {
            data_dir,
            index: Arc::new(RwLock::new(HashMap::new())),
        };
        // No automatic sample seeding; use data_dir traces/rules provided by the user.
        store.refresh_index().await?;
        Ok(store)
    }

    pub async fn list(&self) -> Result<Vec<TraceMeta>> {
        self.refresh_index().await?;
        let mut items: Vec<_> = self.index.read().await.values().cloned().collect();
        items.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(items)
    }

    pub async fn purge_traces(&self, retention: Duration, dry_run: bool) -> Result<PurgeReport> {
        let traces = self.list().await?;
        let traces_dir = traces_dir(&self.data_dir);
        let report = purge_trace_metas(traces, &traces_dir, retention, dry_run).await;

        if !dry_run {
            self.refresh_index().await?;
        }

        Ok(report)
    }

    pub async fn get(&self, trace_id: &str) -> Result<Option<Value>> {
        let meta = match self.resolve_meta(trace_id).await? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        let path = PathBuf::from(&meta.path);
        let raw = read_trace_json_with_limit_async(&path).await?;
        let parse_result = tokio::task::spawn_blocking({
            let raw = raw.clone();
            move || serde_json::from_str::<Value>(&raw)
        })
        .await
        .map_err(|err| anyhow::anyhow!("trace json parse task failed: {}", err))?;
        let value: Value =
            parse_result.with_context(|| format!("invalid trace json: {}", path.display()))?;
        if is_manifest(&value) {
            let mut manifest: TraceManifest = serde_json::from_value(value)
                .with_context(|| format!("invalid trace manifest: {}", path.display()))?;
            manifest.trace_id = meta.trace_id.clone();
            apply_record_total_budget_for_get(&mut manifest);
            let trace_dir = path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf();
            let mut full = build_trace_from_manifest_async(manifest, trace_dir).await?;
            if let Some(obj) = full.as_object_mut() {
                obj.insert("trace_id".to_string(), Value::String(meta.trace_id));
            }
            Ok(Some(full))
        } else {
            let mut legacy = value;
            if looks_like_legacy_trace(&legacy) {
                if let Some(obj) = legacy.as_object_mut() {
                    obj.insert("trace_id".to_string(), Value::String(meta.trace_id));
                }
                apply_legacy_limits(&mut legacy);
            }
            Ok(Some(legacy))
        }
    }

    pub async fn get_manifest(&self, trace_id: &str) -> Result<Option<TraceManifest>> {
        let entry = self.load_manifest_entry(trace_id).await?;
        Ok(entry.map(|(manifest, _)| manifest))
    }

    pub async fn get_records_chunk(
        &self,
        trace_id: &str,
        chunk_index: usize,
    ) -> Result<Option<Vec<Value>>> {
        let Some((manifest, base_dir)) = self.load_manifest_entry(trace_id).await? else {
            return Ok(None);
        };
        get_manifest_records_chunk(manifest, base_dir, chunk_index).await
    }

    pub async fn get_nodes_chunk(
        &self,
        trace_id: &str,
        chunk_index: usize,
    ) -> Result<Option<Vec<TraceNodeChunkEntry>>> {
        let Some((manifest, base_dir)) = self.load_manifest_entry(trace_id).await? else {
            return Ok(None);
        };
        get_manifest_nodes_chunk(manifest, base_dir, chunk_index).await
    }

    pub async fn get_finalize_chunk(&self, trace_id: &str) -> Result<Option<Value>> {
        let Some((manifest, base_dir)) = self.load_manifest_entry(trace_id).await? else {
            return Ok(None);
        };
        get_manifest_finalize_chunk(manifest, base_dir).await
    }

    async fn resolve_meta(&self, trace_id: &str) -> Result<Option<TraceMeta>> {
        if !self.index.read().await.contains_key(trace_id) {
            self.refresh_index().await?;
        }
        Ok(self.index.read().await.get(trace_id).cloned())
    }

    async fn load_manifest_entry(
        &self,
        trace_id: &str,
    ) -> Result<Option<(TraceManifest, PathBuf)>> {
        let meta = match self.resolve_meta(trace_id).await? {
            Some(meta) => meta,
            None => return Ok(None),
        };
        let path = PathBuf::from(&meta.path);
        let raw = read_trace_json_with_limit_async(&path).await?;
        let parse_result = tokio::task::spawn_blocking({
            let raw = raw.clone();
            move || serde_json::from_str::<Value>(&raw)
        })
        .await
        .map_err(|err| anyhow::anyhow!("trace json parse task failed: {}", err))?;
        let value: Value =
            parse_result.with_context(|| format!("invalid trace json: {}", path.display()))?;
        if !is_manifest(&value) {
            return Ok(None);
        }
        let mut manifest: TraceManifest = serde_json::from_value(value)
            .with_context(|| format!("invalid trace manifest: {}", path.display()))?;
        manifest.trace_id = meta.trace_id;
        apply_manifest_budget(&mut manifest);
        let base_dir = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        Ok(Some((manifest, base_dir)))
    }

    pub async fn import_bundle(&self, bundle_path: &Path) -> Result<ImportResult> {
        self.import_bundle_inner(bundle_path, IMPORT_MAX_TOTAL_BYTES)
            .await
    }

    #[cfg(test)]
    pub async fn import_bundle_with_limit(
        &self,
        bundle_path: &Path,
        max_total_bytes: u64,
    ) -> Result<ImportResult> {
        self.import_bundle_inner(bundle_path, max_total_bytes).await
    }

    async fn import_bundle_inner(
        &self,
        bundle_path: &Path,
        max_total_bytes: u64,
    ) -> Result<ImportResult> {
        let bundle_path = bundle_path
            .canonicalize()
            .with_context(|| format!("failed to resolve bundle path: {}", bundle_path.display()))?;
        if !bundle_path.is_dir() {
            return Err(anyhow::anyhow!(
                "bundle path is not a directory: {}",
                bundle_path.display()
            ));
        }

        let data_dir = self.data_dir.clone();
        let result = tokio::task::spawn_blocking(move || {
            import_bundle_files(&data_dir, &bundle_path, max_total_bytes)
        })
        .await??;

        self.refresh_index().await?;

        let index = self.index.read().await;
        let mut trace_ids = Vec::new();
        for path in result.imported_paths {
            let path_string = path.display().to_string();
            if let Some(meta) = index.values().find(|meta| meta.path == path_string) {
                trace_ids.push(meta.trace_id.clone());
            } else {
                warn!(
                    "imported trace metadata not found in index: {}",
                    path.display()
                );
            }
        }

        Ok(ImportResult {
            imported: trace_ids.len(),
            trace_ids,
            rules_imported: result.rules_imported,
        })
    }

    async fn refresh_index(&self) -> Result<()> {
        let data_dir = self.data_dir.clone();
        let index = tokio::task::spawn_blocking(move || build_trace_index(&traces_dir(&data_dir)))
            .await??;

        let mut guard = self.index.write().await;
        *guard = index;
        Ok(())
    }

    // Sample seed disabled (data_dir-only workflow).
}

#[async_trait]
impl TraceBackend for FileTraceBackend {
    async fn list(&self) -> Result<Vec<TraceMeta>> {
        FileTraceBackend::list(self).await
    }

    async fn get(&self, trace_id: &str) -> Result<Option<Value>> {
        FileTraceBackend::get(self, trace_id).await
    }

    async fn get_manifest(&self, trace_id: &str) -> Result<Option<TraceManifest>> {
        FileTraceBackend::get_manifest(self, trace_id).await
    }

    async fn get_records_chunk(
        &self,
        trace_id: &str,
        chunk_index: usize,
    ) -> Result<Option<Vec<Value>>> {
        FileTraceBackend::get_records_chunk(self, trace_id, chunk_index).await
    }

    async fn get_nodes_chunk(
        &self,
        trace_id: &str,
        chunk_index: usize,
    ) -> Result<Option<Vec<TraceNodeChunkEntry>>> {
        FileTraceBackend::get_nodes_chunk(self, trace_id, chunk_index).await
    }

    async fn get_finalize_chunk(&self, trace_id: &str) -> Result<Option<Value>> {
        FileTraceBackend::get_finalize_chunk(self, trace_id).await
    }

    async fn import_bundle(&self, bundle_path: &Path) -> Result<ImportResult> {
        FileTraceBackend::import_bundle(self, bundle_path).await
    }

    async fn purge_traces(&self, retention: Duration, dry_run: bool) -> Result<PurgeReport> {
        FileTraceBackend::purge_traces(self, retention, dry_run).await
    }

    fn data_root(&self) -> &Path {
        &self.data_dir
    }
}

#[derive(Clone)]
pub struct TraceStore {
    backend: Arc<dyn TraceBackend>,
}

impl TraceStore {
    pub async fn new(data_dir: PathBuf) -> Result<Self> {
        let backend = FileTraceBackend::new(data_dir).await?;
        Ok(Self::with_backend(Arc::new(backend)))
    }

    pub fn with_backend(backend: Arc<dyn TraceBackend>) -> Self {
        Self { backend }
    }

    pub async fn list(&self) -> Result<Vec<TraceMeta>> {
        self.backend.list().await
    }

    pub async fn get(&self, trace_id: &str) -> Result<Option<Value>> {
        self.backend.get(trace_id).await
    }

    pub async fn get_manifest(&self, trace_id: &str) -> Result<Option<TraceManifest>> {
        self.backend.get_manifest(trace_id).await
    }

    pub async fn get_records_chunk(
        &self,
        trace_id: &str,
        chunk_index: usize,
    ) -> Result<Option<Vec<Value>>> {
        self.backend.get_records_chunk(trace_id, chunk_index).await
    }

    pub async fn get_nodes_chunk(
        &self,
        trace_id: &str,
        chunk_index: usize,
    ) -> Result<Option<Vec<TraceNodeChunkEntry>>> {
        self.backend.get_nodes_chunk(trace_id, chunk_index).await
    }

    pub async fn get_finalize_chunk(&self, trace_id: &str) -> Result<Option<Value>> {
        self.backend.get_finalize_chunk(trace_id).await
    }

    pub async fn import_bundle(&self, bundle_path: &Path) -> Result<ImportResult> {
        self.backend.import_bundle(bundle_path).await
    }

    pub async fn purge_traces(&self, retention: Duration, dry_run: bool) -> Result<PurgeReport> {
        self.backend.purge_traces(retention, dry_run).await
    }

    pub async fn seed_sample(&self) -> Result<()> {
        let _ = self.list().await?;
        Ok(())
    }

    pub fn data_dir(&self) -> &Path {
        self.backend.data_root()
    }
}

fn traces_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("traces")
}

fn rules_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("rules")
}

// copy_dir_recursive was intentionally omitted to avoid counting existing files.
