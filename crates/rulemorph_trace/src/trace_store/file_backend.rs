use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;

use crate::trace_backend::TraceBackend;
use crate::trace_schema::TraceManifest;

mod import;

use super::chunk_access::{
    get_finalize_chunk as get_manifest_finalize_chunk, get_nodes_chunk as get_manifest_nodes_chunk,
    get_records_chunk as get_manifest_records_chunk,
};
use super::index::build_trace_index;
use super::legacy::{apply_legacy_limits, looks_like_legacy_trace};
use super::manifest_budget::{apply_manifest_budget, apply_record_total_budget_for_get};
use super::manifest_trace::build_trace_from_manifest_async;
use super::meta::{is_manifest, read_trace_value_with_limit_async};
use super::purge::purge_trace_metas;
use super::{ImportResult, PurgeReport, TraceMeta, TraceNodeChunkEntry, rules_dir, traces_dir};

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
        let value = read_trace_value_with_limit_async(&path).await?;
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
        let value = read_trace_value_with_limit_async(&path).await?;
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
