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

#[cfg(test)]
mod tests {
    use super::{
        ChunkBudget, FileTraceBackend, PurgeReport, Result, TraceManifest, TraceMeta, TraceStore,
        apply_legacy_limits_with_thresholds, build_trace_from_manifest_with_budget,
        fallback_trace_id_for_path, path_hash_for_trace_id, resolve_chunk_path,
        resolve_trace_timestamp,
    };
    use crate::trace_id::sanitize_trace_id;
    use crate::trace_schema::{
        TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TRACE_CHUNK_COUNT_HARD_MAX,
        TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX,
        TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TraceChunkRef,
    };
    use crate::{ImportResult, TraceBackend, TraceDetailRef, TraceNodeChunkEntry};
    use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
    use serde_json::{Value, json};
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;

    struct DummyBackend {
        data_dir: std::path::PathBuf,
        list: Vec<TraceMeta>,
    }

    #[async_trait::async_trait]
    impl TraceBackend for DummyBackend {
        async fn list(&self) -> Result<Vec<TraceMeta>> {
            Ok(self.list.clone())
        }

        async fn get(&self, _trace_id: &str) -> Result<Option<Value>> {
            Ok(None)
        }

        async fn get_manifest(&self, _trace_id: &str) -> Result<Option<TraceManifest>> {
            Ok(None)
        }

        async fn get_records_chunk(
            &self,
            _trace_id: &str,
            _chunk_index: usize,
        ) -> Result<Option<Vec<Value>>> {
            Ok(None)
        }

        async fn get_nodes_chunk(
            &self,
            _trace_id: &str,
            _chunk_index: usize,
        ) -> Result<Option<Vec<TraceNodeChunkEntry>>> {
            Ok(None)
        }

        async fn get_finalize_chunk(&self, _trace_id: &str) -> Result<Option<Value>> {
            Ok(None)
        }

        async fn import_bundle(&self, _bundle_path: &Path) -> Result<ImportResult> {
            Ok(ImportResult {
                imported: 0,
                trace_ids: Vec::new(),
                rules_imported: 0,
            })
        }

        async fn purge_traces(&self, _retention: Duration, _dry_run: bool) -> Result<PurgeReport> {
            Ok(PurgeReport {
                purged: Vec::new(),
                failed: Vec::new(),
            })
        }

        fn data_root(&self) -> &Path {
            &self.data_dir
        }
    }

    #[test]
    fn fallback_trace_id_uses_hash_when_stem_missing() {
        let path = Path::new("/");
        let trace_id = fallback_trace_id_for_path(path);
        assert!(trace_id.starts_with("trace-"));
    }

    #[test]
    fn sanitize_trace_id_rejects_dot_only() {
        assert!(sanitize_trace_id(".").is_empty());
        assert!(sanitize_trace_id("..").is_empty());
    }

    #[test]
    fn hash_uses_traces_relative_path_when_possible() {
        let path_a = Path::new("/tmp/a/traces/2026/01/trace.json");
        let path_b = Path::new("/var/b/traces/2026/01/trace.json");
        assert_eq!(
            path_hash_for_trace_id(path_a),
            path_hash_for_trace_id(path_b)
        );
    }

    #[test]
    fn resolve_chunk_path_rejects_parent_dirs() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path().join("trace");
        std::fs::create_dir_all(&base).expect("create base dir");

        let err = resolve_chunk_path(&base, "../escape.json").expect_err("should reject");
        assert!(err.to_string().contains("relative"));
    }

    #[test]
    fn resolve_chunk_path_rejects_absolute_paths() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path().join("trace");
        std::fs::create_dir_all(&base).expect("create base dir");

        let err = resolve_chunk_path(&base, "/tmp/escape.json").expect_err("should reject");
        assert!(err.to_string().contains("relative"));
    }

    #[test]
    fn resolve_chunk_path_accepts_relative_paths() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path().join("trace");
        std::fs::create_dir_all(&base).expect("create base dir");
        let path = base.join("records-0001.ndjson");
        std::fs::write(&path, b"{}").expect("write chunk");

        let resolved = resolve_chunk_path(&base, "records-0001.ndjson").expect("resolve");
        let base = base.canonicalize().expect("canonicalize base");
        assert!(resolved.starts_with(&base));
    }

    #[tokio::test]
    async fn trace_store_with_backend_uses_backend_list() -> Result<()> {
        let temp = tempdir()?;
        let data_dir = temp.path().to_path_buf();
        let meta = TraceMeta {
            trace_id: "trace-001".to_string(),
            status: "ok".to_string(),
            timestamp: None,
            duration_us: None,
            rule: None,
            summary: None,
            path: "traces/2026/02/03/trace-001/trace.json".to_string(),
        };
        let backend = DummyBackend {
            data_dir: data_dir.clone(),
            list: vec![meta.clone()],
        };
        let store = TraceStore::with_backend(Arc::new(backend));
        let traces = store.list().await?;
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].trace_id, "trace-001");
        assert_eq!(store.data_dir(), data_dir.as_path());
        Ok(())
    }

    #[tokio::test]
    async fn manifest_downgrades_when_chunk_budget_exceeded() -> Result<()> {
        let temp = tempdir()?;
        let trace_dir = temp.path().join("traces/2026/02/03/trace-over-budget");
        fs::create_dir_all(&trace_dir)?;

        let mut records = Vec::new();
        for index in 0..(TRACE_CHUNK_COUNT_HARD_MAX + 1) {
            records.push(json!({
                "path": format!("records-{index:04}.ndjson"),
                "format": "ndjson",
                "compression": "none",
                "record_start": 0,
                "record_end": 0
            }));
        }

        fs::write(
            trace_dir.join("trace.json"),
            serde_json::to_vec(&json!({
                "trace_schema_version": 1,
                "trace_id": "trace-over-budget",
                "status": "ok",
                "detail": {
                    "layout": "records_inline",
                    "status": "full",
                    "reason": [],
                    "records": records,
                    "nodes": []
                }
            }))?,
        )?;

        let store = TraceStore::new(temp.path().to_path_buf()).await?;
        let manifest = store
            .get_manifest("trace-over-budget")
            .await?
            .expect("manifest");
        let detail = manifest.detail.expect("detail");
        assert_eq!(detail.status, "basic");
        assert!(
            detail
                .reason
                .iter()
                .any(|reason| reason == "budget_exceeded")
        );
        assert!(detail.records.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn manifest_downgrades_when_total_bytes_exceeded() -> Result<()> {
        let temp = tempdir()?;
        let trace_dir = temp.path().join("traces/2026/02/03/trace-bytes-budget");
        fs::create_dir_all(&trace_dir)?;

        fs::write(
            trace_dir.join("trace.json"),
            serde_json::to_vec(&json!({
                "trace_schema_version": 1,
                "trace_id": "trace-bytes-budget",
                "status": "ok",
                "detail": {
                    "layout": "records_inline",
                    "status": "full",
                    "reason": [],
                    "records": [
                        {
                            "path": "records-0001.ndjson",
                            "format": "ndjson",
                            "compression": "none",
                            "record_start": 0,
                            "record_end": 0,
                            "bytes": TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX as u64 + 1
                        }
                    ],
                    "nodes": []
                }
            }))?,
        )?;

        let store = TraceStore::new(temp.path().to_path_buf()).await?;
        let manifest = store
            .get_manifest("trace-bytes-budget")
            .await?
            .expect("manifest");
        let detail = manifest.detail.expect("detail");
        assert_eq!(detail.status, "basic");
        assert!(
            detail
                .reason
                .iter()
                .any(|reason| reason == "budget_exceeded")
        );

        Ok(())
    }

    #[tokio::test]
    async fn purge_traces_removes_old_entries() -> Result<()> {
        let temp = tempdir()?;
        let data_dir = temp.path().to_path_buf();
        let old_dir = data_dir.join("traces/2026/01/01/trace-old");
        let new_dir = data_dir.join("traces/2026/01/01/trace-new");
        fs::create_dir_all(&old_dir)?;
        fs::create_dir_all(&new_dir)?;

        let old_ts =
            (Utc::now() - chrono::Duration::days(20)).to_rfc3339_opts(SecondsFormat::Secs, true);
        let new_ts =
            (Utc::now() - chrono::Duration::days(2)).to_rfc3339_opts(SecondsFormat::Secs, true);

        fs::write(
            old_dir.join("trace.json"),
            serde_json::to_vec(&json!({
                "trace_schema_version": 1,
                "trace_id": "trace-old",
                "timestamp": old_ts,
                "status": "ok"
            }))?,
        )?;
        fs::write(
            new_dir.join("trace.json"),
            serde_json::to_vec(&json!({
                "trace_schema_version": 1,
                "trace_id": "trace-new",
                "timestamp": new_ts,
                "status": "ok"
            }))?,
        )?;

        let store = TraceStore::new(data_dir.clone()).await?;
        let dry_run = store
            .purge_traces(Duration::from_secs(10 * 86_400), true)
            .await?;
        assert!(
            dry_run
                .purged
                .iter()
                .any(|meta| meta.trace_id == "trace-old")
        );
        assert!(
            !dry_run
                .purged
                .iter()
                .any(|meta| meta.trace_id == "trace-new")
        );
        assert!(dry_run.failed.is_empty());
        assert!(old_dir.exists());

        let purged = store
            .purge_traces(Duration::from_secs(10 * 86_400), false)
            .await?;
        assert!(
            purged
                .purged
                .iter()
                .any(|meta| meta.trace_id == "trace-old")
        );
        assert!(purged.failed.is_empty());
        assert!(!old_dir.exists());
        assert!(new_dir.exists());

        Ok(())
    }

    #[tokio::test]
    async fn resolve_trace_timestamp_ignores_far_future() -> Result<()> {
        let temp = tempdir()?;
        let trace_dir = temp.path().join("traces/2026/01/01/trace-future");
        fs::create_dir_all(&trace_dir)?;
        let future_dt = Utc::now() + ChronoDuration::days(365);
        let future_ts = future_dt.to_rfc3339_opts(SecondsFormat::Secs, true);
        let trace_path = trace_dir.join("trace.json");
        fs::write(
            &trace_path,
            serde_json::to_vec(&json!({
                "trace_schema_version": 1,
                "trace_id": "trace-future",
                "timestamp": future_ts,
                "status": "ok"
            }))?,
        )?;

        let meta = TraceMeta {
            trace_id: "trace-future".to_string(),
            status: "ok".to_string(),
            timestamp: Some(future_ts),
            duration_us: None,
            rule: None,
            summary: None,
            path: trace_path.to_string_lossy().to_string(),
        };

        let resolved = resolve_trace_timestamp(&meta, &trace_path)
            .await
            .expect("timestamp");
        assert!(resolved < std::time::SystemTime::from(future_dt));
        Ok(())
    }

    #[tokio::test]
    async fn manifest_downgrades_when_unknown_uncompressed_bytes_exceed_budget() -> Result<()> {
        let temp = tempdir()?;
        let trace_dir = temp.path().join("traces/2026/02/03/trace-unknown-bytes");
        fs::create_dir_all(&trace_dir)?;

        let unknown_chunks = TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX
            / TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX
            + 1;
        let records: Vec<Value> = (0..unknown_chunks)
            .map(|index| {
                json!({
                    "path": format!("records-{index:04}.ndjson.zst"),
                    "format": "ndjson",
                    "compression": "zstd",
                    "record_start": index as u64,
                    "record_end": index as u64,
                    "bytes": 1024
                })
            })
            .collect();

        fs::write(
            trace_dir.join("trace.json"),
            serde_json::to_vec(&json!({
                "trace_schema_version": 1,
                "trace_id": "trace-unknown-bytes",
                "status": "ok",
                "max_chunk_bytes_uncompressed": TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX as u64,
                "detail": {
                    "layout": "records_inline",
                    "status": "full",
                    "reason": [],
                    "records": records,
                    "nodes": []
                }
            }))?,
        )?;

        let store = TraceStore::new(temp.path().to_path_buf()).await?;
        let manifest = store
            .get_manifest("trace-unknown-bytes")
            .await?
            .expect("manifest");
        let detail = manifest.detail.expect("detail");
        assert_eq!(detail.status, "basic");
        assert!(
            detail
                .reason
                .iter()
                .any(|reason| reason == "budget_exceeded")
        );

        Ok(())
    }

    #[tokio::test]
    async fn manifest_downgrades_when_record_total_exceeded() -> Result<()> {
        let temp = tempdir()?;
        let trace_dir = temp.path().join("traces/2026/02/03/trace-record-budget");
        fs::create_dir_all(&trace_dir)?;

        fs::write(
            trace_dir.join("trace.json"),
            serde_json::to_vec(&json!({
                "trace_schema_version": 1,
                "trace_id": "trace-record-budget",
                "status": "ok",
                "summary": {
                    "record_total": TRACE_RECORD_COUNT_HARD_MAX as u64 + 1
                },
                "detail": {
                    "layout": "records_inline",
                    "status": "full",
                    "reason": [],
                    "records": [],
                    "nodes": []
                }
            }))?,
        )?;

        let store = TraceStore::new(temp.path().to_path_buf()).await?;
        let manifest = store
            .get_manifest("trace-record-budget")
            .await?
            .expect("manifest");
        let detail = manifest.detail.expect("detail");
        assert_eq!(detail.status, "basic");
        assert!(
            detail
                .reason
                .iter()
                .any(|reason| reason == "budget_exceeded")
        );

        let trace = store.get("trace-record-budget").await?.expect("trace");
        let detail = trace
            .get("detail")
            .and_then(|value| value.as_object())
            .expect("detail object");
        assert_eq!(
            detail.get("status").and_then(|value| value.as_str()),
            Some("basic")
        );
        let reasons = detail
            .get("reason")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(
            reasons
                .iter()
                .any(|value| value.as_str() == Some("budget_exceeded"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn manifest_downgrades_when_node_total_exceeded() -> Result<()> {
        let temp = tempdir()?;
        let trace_dir = temp.path().join("traces/2026/02/03/trace-node-budget");
        fs::create_dir_all(&trace_dir)?;

        fs::write(
            trace_dir.join("trace.json"),
            serde_json::to_vec(&json!({
                "trace_schema_version": 1,
                "trace_id": "trace-node-budget",
                "status": "ok",
                "detail": {
                    "layout": "records_nodes_split",
                    "status": "full",
                    "reason": [],
                    "records": [],
                    "nodes": [
                        {
                            "path": "nodes-0001.ndjson",
                            "format": "ndjson",
                            "compression": "none",
                            "node_start": 0,
                            "node_end": TRACE_NODE_COUNT_HARD_MAX as u64
                        }
                    ]
                }
            }))?,
        )?;

        let store = TraceStore::new(temp.path().to_path_buf()).await?;
        let manifest = store
            .get_manifest("trace-node-budget")
            .await?
            .expect("manifest");
        let detail = manifest.detail.expect("detail");
        assert_eq!(detail.status, "basic");
        assert!(
            detail
                .reason
                .iter()
                .any(|reason| reason == "budget_exceeded")
        );

        Ok(())
    }

    #[test]
    fn legacy_trace_downgrades_on_record_limit() -> Result<()> {
        let mut legacy = json!({
            "trace_id": "legacy-over-limit",
            "records": [
                { "index": 0, "status": "ok" },
                { "index": 1, "status": "ok" }
            ]
        });

        apply_legacy_limits_with_thresholds(&mut legacy, 1, 10);

        let detail = legacy
            .get("detail")
            .and_then(|value| value.as_object())
            .expect("detail object");
        assert_eq!(
            detail.get("status").and_then(|value| value.as_str()),
            Some("basic")
        );
        let reasons = detail
            .get("reason")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(
            reasons
                .iter()
                .any(|value| value.as_str() == Some("budget_exceeded"))
        );
        let records = legacy
            .get("records")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(records.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn trace_store_downgrades_detail_on_oversized_chunk() -> Result<()> {
        let temp = tempdir()?;
        let data_dir = temp.path();
        let trace_dir = data_dir.join("traces/2026/01/07/trace-oversized");
        fs::create_dir_all(&trace_dir)?;

        let oversized = TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX + 1024;
        let payload = format!(
            "{{\"index\":0,\"payload\":\"{}\"}}\n",
            "x".repeat(oversized)
        );
        let compressed = zstd::stream::encode_all(payload.as_bytes(), 3)?;
        fs::write(trace_dir.join("records-0001.ndjson.zst"), compressed)?;

        fs::write(
            trace_dir.join("trace.json"),
            serde_json::to_vec(&json!({
                "trace_schema_version": 1,
                "trace_id": "trace-oversized",
                "status": "ok",
                "detail": {
                    "layout": "records_inline",
                    "status": "full",
                    "records": [
                        {
                            "path": "records-0001.ndjson.zst",
                            "format": "ndjson",
                            "compression": "zstd",
                            "bytes": 1024,
                            "record_start": 0,
                            "record_end": 0
                        }
                    ],
                    "nodes": []
                }
            }))?,
        )?;

        let store = TraceStore::new(data_dir.to_path_buf()).await?;
        let trace = store
            .get("trace-oversized")
            .await?
            .expect("trace should exist");
        let detail = trace
            .get("detail")
            .and_then(|value| value.as_object())
            .expect("detail object");
        assert_eq!(
            detail.get("status").and_then(|value| value.as_str()),
            Some("basic")
        );
        let reasons = detail
            .get("reason")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(
            reasons
                .iter()
                .any(|value| value.as_str() == Some("chunk_error"))
        );
        assert!(
            reasons
                .iter()
                .any(|value| value.as_str() == Some("chunk_too_large"))
        );
        let detail_records = detail
            .get("records")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(detail_records.is_empty());
        let detail_nodes = detail
            .get("nodes")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(detail_nodes.is_empty());
        assert!(detail.get("finalize").is_none());
        let records = trace
            .get("records")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(records.is_empty());
        assert!(trace.get("finalize").is_none());

        Ok(())
    }

    #[test]
    fn trace_store_downgrades_on_total_bytes_budget() -> Result<()> {
        let temp = tempdir()?;
        let trace_dir = temp.path().join("trace-budget");
        fs::create_dir_all(&trace_dir)?;

        let payload = format!("{{\"index\":0,\"payload\":\"{}\"}}\n", "x".repeat(64));
        let payload_len = payload.as_bytes().len();
        fs::write(trace_dir.join("records-0001.ndjson"), payload.as_bytes())?;
        fs::write(trace_dir.join("records-0002.ndjson"), payload.as_bytes())?;

        let max_chunk_bytes = payload_len + 8;
        let manifest = TraceManifest {
            trace_schema_version: 1,
            trace_id: "trace-budget".to_string(),
            timestamp: None,
            status: Some("ok".to_string()),
            rule: None,
            input_format: None,
            summary: None,
            max_chunk_bytes_uncompressed: Some(max_chunk_bytes as u64),
            detail: Some(TraceDetailRef {
                layout: "records_inline".to_string(),
                status: "full".to_string(),
                reason: Vec::new(),
                records: vec![
                    TraceChunkRef {
                        path: "records-0001.ndjson".to_string(),
                        format: "ndjson".to_string(),
                        compression: "none".to_string(),
                        record_start: None,
                        record_end: None,
                        node_start: None,
                        node_end: None,
                        bytes: None,
                        bytes_uncompressed: None,
                    },
                    TraceChunkRef {
                        path: "records-0002.ndjson".to_string(),
                        format: "ndjson".to_string(),
                        compression: "none".to_string(),
                        record_start: None,
                        record_end: None,
                        node_start: None,
                        node_end: None,
                        bytes: None,
                        bytes_uncompressed: None,
                    },
                ],
                nodes: Vec::new(),
                finalize: None,
            }),
            masking: None,
            rule_source: None,
        };

        let budget = ChunkBudget {
            remaining_bytes: payload_len + 1,
            remaining_chunks: 10,
        };
        let trace = build_trace_from_manifest_with_budget(&manifest, &trace_dir, budget)?;
        let detail = trace
            .get("detail")
            .and_then(|value| value.as_object())
            .expect("detail object");
        assert_eq!(
            detail.get("status").and_then(|value| value.as_str()),
            Some("basic")
        );
        let reasons = detail
            .get("reason")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(
            reasons
                .iter()
                .any(|value| value.as_str() == Some("budget_exceeded"))
        );
        let detail_records = detail
            .get("records")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(detail_records.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn import_bundle_rejects_total_bytes_limit() -> Result<()> {
        let temp = tempdir()?;
        let data_dir = temp.path().join("data");
        let bundle_dir = temp.path().join("bundle");
        let bundle_traces = bundle_dir.join("traces/2026/01/10/trace-total-limit");
        fs::create_dir_all(&bundle_traces)?;

        let trace_payload = serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-total-limit",
            "status": "ok",
            "timestamp": "2026-01-10T00:00:00Z"
        }))?;
        fs::write(bundle_traces.join("trace.json"), &trace_payload)?;
        let records_payload = vec![b'x'; 64];
        fs::write(bundle_traces.join("records-0001.ndjson"), &records_payload)?;

        let max_total_bytes =
            (trace_payload.len().saturating_add(records_payload.len()) as u64).saturating_sub(1);
        let store = FileTraceBackend::new(data_dir).await?;
        let err = store
            .import_bundle_with_limit(&bundle_dir, max_total_bytes)
            .await
            .expect_err("total bytes should be rejected");
        assert!(err.to_string().contains("max total bytes"));

        Ok(())
    }
}

// copy_dir_recursive was intentionally omitted to avoid counting existing files.
