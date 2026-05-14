use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tracing::warn;
use walkdir::WalkDir;

use crate::trace_backend::TraceBackend;
use crate::trace_id::{sanitize_trace_id, trace_id_is_placeholder};
use crate::trace_schema::{
    RuleMeta, TRACE_JSON_MAX_BYTES, TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX,
    TraceManifest, TraceSummary,
};

mod chunk_read;
mod import_bundle;
mod import_path;
mod manifest_budget;
mod purge;
mod trace_id_path;

#[cfg(test)]
use self::chunk_read::resolve_chunk_path;
use self::chunk_read::{
    ChunkBudget, count_inline_nodes, normalize_inline_nodes_value, parse_node_chunk_entry,
    parse_record_index, read_json_chunk, read_ndjson_chunk,
};
use self::import_bundle::{IMPORT_MAX_TOTAL_BYTES, import_bundle_files};
use self::manifest_budget::{
    apply_manifest_budget, apply_record_total_budget_for_get, resolve_max_chunk_bytes,
};
use self::purge::purge_trace_metas;
#[cfg(test)]
use self::purge::resolve_trace_timestamp;
use self::trace_id_path::{
    fallback_trace_id_for_path, hash_trace_id_for_path, path_hash_for_trace_id,
};

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
        let detail = match manifest.detail.as_ref() {
            Some(detail) => detail,
            None => return Ok(None),
        };
        if detail.status != "full" {
            return Ok(None);
        }
        let chunk = match detail.records.get(chunk_index) {
            Some(chunk) => chunk.clone(),
            None => return Ok(None),
        };
        let max_chunk_bytes = resolve_max_chunk_bytes(&manifest);
        let chunk_path = chunk.path.clone();
        let result = tokio::task::spawn_blocking(move || {
            read_ndjson_chunk(
                &base_dir,
                &chunk,
                max_chunk_bytes,
                TRACE_RECORD_COUNT_HARD_MAX,
            )
        })
        .await
        .map_err(|err| anyhow::anyhow!("trace record chunk task failed: {}", err))??;
        if result.had_error || result.size_exceeded || result.limit_exceeded {
            return Err(anyhow::anyhow!(
                "trace record chunk failed to load: {}",
                chunk_path
            ));
        }
        Ok(Some(result.value))
    }

    pub async fn get_nodes_chunk(
        &self,
        trace_id: &str,
        chunk_index: usize,
    ) -> Result<Option<Vec<TraceNodeChunkEntry>>> {
        let Some((manifest, base_dir)) = self.load_manifest_entry(trace_id).await? else {
            return Ok(None);
        };
        let detail = match manifest.detail.as_ref() {
            Some(detail) => detail,
            None => return Ok(None),
        };
        if detail.status != "full" {
            return Ok(None);
        }
        if detail.layout != "records_nodes_split" {
            warn!(
                "node chunks present but layout is {}; skipping nodes chunk",
                detail.layout
            );
            return Ok(None);
        }
        let chunk = match detail.nodes.get(chunk_index) {
            Some(chunk) => chunk.clone(),
            None => return Ok(None),
        };
        let max_chunk_bytes = resolve_max_chunk_bytes(&manifest);
        let chunk_path = chunk.path.clone();
        let result = tokio::task::spawn_blocking(move || {
            read_ndjson_chunk(
                &base_dir,
                &chunk,
                max_chunk_bytes,
                TRACE_NODE_COUNT_HARD_MAX,
            )
        })
        .await
        .map_err(|err| anyhow::anyhow!("trace node chunk task failed: {}", err))??;
        if result.had_error || result.size_exceeded || result.limit_exceeded {
            return Err(anyhow::anyhow!(
                "trace node chunk failed to load: {}",
                chunk_path
            ));
        }
        let mut entries = Vec::new();
        let mut last_record_index: Option<u64> = None;
        for value in result.value {
            let entry = parse_node_chunk_entry(value);
            if entry.record_index.is_none() && entry.record_index_present {
                warn!("node chunk entry has invalid record_index; skipping");
                continue;
            }
            let record_index = match entry.record_index.or(last_record_index) {
                Some(index) => index,
                None => {
                    warn!("node chunk entry missing record_index; skipping");
                    continue;
                }
            };
            if entry.record_index.is_some() {
                last_record_index = entry.record_index;
            }
            entries.push(TraceNodeChunkEntry {
                record_index,
                node: entry.node,
            });
        }
        Ok(Some(entries))
    }

    pub async fn get_finalize_chunk(&self, trace_id: &str) -> Result<Option<Value>> {
        let Some((manifest, base_dir)) = self.load_manifest_entry(trace_id).await? else {
            return Ok(None);
        };
        let detail = match manifest.detail.as_ref() {
            Some(detail) => detail,
            None => return Ok(None),
        };
        if detail.status != "full" {
            return Ok(None);
        }
        let chunk = match detail.finalize.as_ref() {
            Some(chunk) => chunk.clone(),
            None => return Ok(None),
        };
        let max_chunk_bytes = resolve_max_chunk_bytes(&manifest);
        let chunk_path = chunk.path.clone();
        let result = tokio::task::spawn_blocking(move || {
            read_json_chunk(&base_dir, &chunk, max_chunk_bytes)
        })
        .await
        .map_err(|err| anyhow::anyhow!("trace finalize chunk task failed: {}", err))??;
        if result.had_error || result.size_exceeded || result.limit_exceeded {
            return Err(anyhow::anyhow!(
                "trace finalize chunk failed to load: {}",
                chunk_path
            ));
        }
        Ok(result.value)
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
        let index = tokio::task::spawn_blocking(move || -> Result<HashMap<String, TraceMeta>> {
            let mut metas = Vec::new();
            let dir = traces_dir(&data_dir);
            if !dir.exists() {
                return Ok(HashMap::new());
            }
            for entry in WalkDir::new(&dir)
                .into_iter()
                .filter_entry(|entry| {
                    !(entry.file_type().is_dir() && entry.file_name().to_string_lossy() == "blobs")
                })
                .filter_map(|e| e.ok())
            {
                if !entry.file_type().is_file() {
                    continue;
                }
                let path = entry.path();
                if !is_trace_meta_candidate(path) {
                    continue;
                }
                match parse_trace_meta(path) {
                    Ok(meta) => metas.push(meta),
                    Err(err) => {
                        warn!("failed to parse trace metadata {}: {}", path.display(), err);
                    }
                }
            }
            let original_ids: HashSet<String> =
                metas.iter().map(|meta| meta.trace_id.clone()).collect();
            let mut by_id: HashMap<String, Vec<TraceMeta>> = HashMap::new();
            for meta in metas {
                by_id.entry(meta.trace_id.clone()).or_default().push(meta);
            }

            let mut used_ids = HashSet::new();
            let mut map = HashMap::new();
            let mut keys: Vec<String> = by_id.keys().cloned().collect();
            keys.sort();

            for base_id in keys {
                let mut group = by_id.remove(&base_id).unwrap_or_default();
                group.sort_by(|a, b| a.path.cmp(&b.path));
                if group.len() == 1 {
                    let meta = group.pop().expect("single meta");
                    used_ids.insert(base_id.clone());
                    map.insert(base_id, meta);
                    continue;
                }

                for mut meta in group {
                    let hash = path_hash_for_trace_id(Path::new(&meta.path));
                    let mut candidate = format!("{base_id}-dup-{hash:x}");
                    let mut counter = 0usize;
                    while used_ids.contains(&candidate) || original_ids.contains(&candidate) {
                        counter = counter.saturating_add(1);
                        candidate = format!("{base_id}-dup-{hash:x}-{counter}");
                    }
                    warn!(
                        "trace_id collision {} at {}; using {}",
                        base_id, meta.path, candidate
                    );
                    meta.trace_id = candidate.clone();
                    used_ids.insert(candidate.clone());
                    map.insert(candidate, meta);
                }
            }

            Ok(map)
        })
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

async fn read_trace_json_with_limit_async(path: &Path) -> Result<String> {
    let metadata = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("failed to read trace metadata: {}", path.display()))?;
    if metadata.len() > TRACE_JSON_MAX_BYTES {
        return Err(anyhow::anyhow!(
            "trace json exceeds max bytes: {} > {}",
            metadata.len(),
            TRACE_JSON_MAX_BYTES
        ));
    }
    tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read trace: {}", path.display()))
}

fn read_trace_json_with_limit(path: &Path) -> Result<String> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("failed to read trace metadata: {}", path.display()))?;
    if metadata.len() > TRACE_JSON_MAX_BYTES {
        return Err(anyhow::anyhow!(
            "trace json exceeds max bytes: {} > {}",
            metadata.len(),
            TRACE_JSON_MAX_BYTES
        ));
    }
    std::fs::read_to_string(path)
        .with_context(|| format!("failed to read trace: {}", path.display()))
}

fn parse_trace_meta(path: &Path) -> Result<TraceMeta> {
    let raw = read_trace_json_with_limit(path)?;
    let value: Value = serde_json::from_str(&raw)
        .with_context(|| format!("invalid trace json: {}", path.display()))?;

    if is_manifest(&value) {
        let manifest: TraceManifest = serde_json::from_value(value)
            .with_context(|| format!("invalid trace manifest: {}", path.display()))?;
        return parse_manifest_meta(&manifest, path);
    }

    if !looks_like_legacy_trace(&value) {
        return Err(anyhow::anyhow!("not a trace file"));
    }

    let raw_trace_id = value
        .get("trace_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string()
        });
    let mut trace_id = sanitize_trace_id(&raw_trace_id);
    if trace_id.is_empty() {
        trace_id = fallback_trace_id_for_path(path);
        warn!(
            "legacy trace_id sanitized to empty; using {} from {}",
            trace_id,
            path.display()
        );
    } else if trace_id_is_placeholder(&trace_id) {
        let fallback = hash_trace_id_for_path(path);
        warn!(
            "legacy trace_id is insufficient; using {} from {}",
            fallback,
            path.display()
        );
        trace_id = fallback;
    } else if trace_id != raw_trace_id {
        warn!(
            "legacy trace_id sanitized from {} to {}",
            raw_trace_id, trace_id
        );
    }

    let status = value
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("ok")
        .to_string();

    let timestamp = value
        .get("timestamp")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let duration_us = value
        .get("summary")
        .and_then(|s| s.get("duration_us"))
        .and_then(|v| v.as_u64())
        .or_else(|| {
            value
                .get("summary")
                .and_then(|s| s.get("duration_ms"))
                .and_then(|v| v.as_u64())
                .map(|v| v.saturating_mul(1000))
        })
        .or_else(|| value.get("duration_us").and_then(|v| v.as_u64()))
        .or_else(|| {
            value
                .get("duration_ms")
                .and_then(|v| v.as_u64())
                .map(|v| v.saturating_mul(1000))
        });

    let rule = value.get("rule").map(|rule| RuleMeta {
        name: rule
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        path: rule
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        r#type: rule
            .get("type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        version: rule
            .get("version")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8),
    });

    let summary = value.get("summary").map(|summary| TraceSummary {
        record_total: summary.get("record_total").and_then(|v| v.as_u64()),
        record_success: summary.get("record_success").and_then(|v| v.as_u64()),
        record_failed: summary.get("record_failed").and_then(|v| v.as_u64()),
        duration_ms: summary.get("duration_ms").and_then(|v| v.as_u64()),
        duration_us: summary.get("duration_us").and_then(|v| v.as_u64()),
    });

    Ok(TraceMeta {
        trace_id,
        status,
        timestamp,
        duration_us,
        rule,
        summary,
        path: path.display().to_string(),
    })
}

fn is_manifest(value: &Value) -> bool {
    value.get("trace_schema_version").is_some()
}

fn looks_like_legacy_trace(value: &Value) -> bool {
    value.get("trace_id").is_some() || value.get("records").is_some() || value.get("rule").is_some()
}

fn apply_legacy_limits(legacy: &mut Value) {
    apply_legacy_limits_with_thresholds(
        legacy,
        TRACE_RECORD_COUNT_HARD_MAX,
        TRACE_NODE_COUNT_HARD_MAX,
    );
}

fn apply_legacy_limits_with_thresholds(legacy: &mut Value, record_limit: usize, node_limit: usize) {
    let (record_count, node_count) = legacy_counts(legacy);
    if record_count <= record_limit && node_count <= node_limit {
        return;
    }
    if let Some(obj) = legacy.as_object_mut() {
        obj.insert("records".to_string(), Value::Array(Vec::new()));
        obj.remove("finalize");
        obj.remove("nodes");
        obj.insert(
            "detail".to_string(),
            json!({
                "layout": "records_inline",
                "status": "basic",
                "reason": ["budget_exceeded"],
                "records": [],
                "nodes": []
            }),
        );
    }
}

fn legacy_counts(legacy: &Value) -> (usize, usize) {
    let mut record_count = 0usize;
    let mut node_count = 0usize;
    if let Some(records) = legacy.get("records").and_then(|value| value.as_array()) {
        record_count = records.len();
        for record in records {
            if let Some(nodes) = record.get("nodes").and_then(|value| value.as_array()) {
                node_count = node_count.saturating_add(nodes.len());
            }
        }
    }
    if let Some(nodes) = legacy.get("nodes").and_then(|value| value.as_array()) {
        node_count = node_count.saturating_add(nodes.len());
    }
    (record_count, node_count)
}

fn parse_manifest_meta(manifest: &TraceManifest, path: &Path) -> Result<TraceMeta> {
    let duration_us = manifest
        .summary
        .as_ref()
        .and_then(|summary| summary.duration_us)
        .or_else(|| {
            manifest
                .summary
                .as_ref()
                .and_then(|summary| summary.duration_ms)
                .map(|value| value.saturating_mul(1000))
        });

    let raw_trace_id = manifest.trace_id.clone();
    let mut trace_id = sanitize_trace_id(&raw_trace_id);
    if trace_id.is_empty() {
        trace_id = fallback_trace_id_for_path(path);
        warn!(
            "manifest trace_id sanitized to empty; using {} from {}",
            trace_id,
            path.display()
        );
    } else if trace_id_is_placeholder(&trace_id) {
        let fallback = hash_trace_id_for_path(path);
        warn!(
            "manifest trace_id is insufficient; using {} from {}",
            fallback,
            path.display()
        );
        trace_id = fallback;
    } else if trace_id != raw_trace_id {
        warn!(
            "manifest trace_id sanitized from {} to {}",
            raw_trace_id, trace_id
        );
    }

    Ok(TraceMeta {
        trace_id,
        status: manifest.status.clone().unwrap_or_else(|| "ok".to_string()),
        timestamp: manifest.timestamp.clone(),
        duration_us,
        rule: manifest.rule.clone(),
        summary: manifest.summary.clone(),
        path: path.display().to_string(),
    })
}

fn build_trace_from_manifest(manifest: &TraceManifest, base_dir: &Path) -> Result<Value> {
    build_trace_from_manifest_with_budget(manifest, base_dir, ChunkBudget::new())
}

fn build_trace_from_manifest_with_budget(
    manifest: &TraceManifest,
    base_dir: &Path,
    mut budget: ChunkBudget,
) -> Result<Value> {
    let mut trace = serde_json::to_value(manifest)?;
    let mut records = Vec::new();
    let max_chunk_bytes = resolve_max_chunk_bytes(manifest);

    let detail = match &manifest.detail {
        Some(detail) => detail,
        None => {
            if let Some(obj) = trace.as_object_mut() {
                obj.insert("records".to_string(), Value::Array(Vec::new()));
            }
            return Ok(trace);
        }
    };

    if detail.status != "full" {
        if let Some(obj) = trace.as_object_mut() {
            obj.insert("records".to_string(), Value::Array(Vec::new()));
            obj.remove("finalize");
            if let Some(detail_obj) = obj
                .get_mut("detail")
                .and_then(|value| value.as_object_mut())
            {
                detail_obj.insert("records".to_string(), Value::Array(Vec::new()));
                detail_obj.insert("nodes".to_string(), Value::Array(Vec::new()));
                detail_obj.remove("finalize");
            }
        }
        return Ok(trace);
    }

    let mut detail_status = detail.status.clone();
    let mut detail_reason = detail.reason.clone();
    let mut chunk_error = false;
    let mut budget_exceeded = false;
    let mut size_exceeded = false;
    let mut remaining_records = TRACE_RECORD_COUNT_HARD_MAX;
    let mut remaining_nodes = TRACE_NODE_COUNT_HARD_MAX;

    for chunk in &detail.records {
        if !budget.consume_chunk() {
            budget_exceeded = true;
            chunk_error = true;
            break;
        }
        let lines = read_ndjson_chunk(base_dir, chunk, max_chunk_bytes, remaining_records)?;
        if lines.had_error {
            chunk_error = true;
        }
        if lines.size_exceeded {
            size_exceeded = true;
            chunk_error = true;
            break;
        }
        if lines.limit_exceeded {
            budget_exceeded = true;
            chunk_error = true;
            break;
        }
        if !budget.consume_bytes(lines.bytes) {
            budget_exceeded = true;
            chunk_error = true;
        }
        if !budget_exceeded {
            remaining_records = remaining_records.saturating_sub(lines.value.len());
            records.extend(lines.value);
        }
        if budget_exceeded {
            break;
        }
    }

    if !chunk_error {
        for record in &mut records {
            if let Some(obj) = record.as_object_mut() {
                if let Some(nodes_value) = obj.get("nodes").cloned() {
                    let normalized = normalize_inline_nodes_value(&nodes_value);
                    obj.insert("nodes".to_string(), Value::Array(normalized));
                }
            }
        }
    }

    let inline_nodes = if !chunk_error {
        count_inline_nodes(&records, TRACE_NODE_COUNT_HARD_MAX)
    } else {
        0
    };
    if inline_nodes > TRACE_NODE_COUNT_HARD_MAX {
        budget_exceeded = true;
        chunk_error = true;
    }
    if detail.layout == "records_nodes_split" && !chunk_error {
        remaining_nodes = remaining_nodes.saturating_sub(inline_nodes);
    }

    if !detail.nodes.is_empty() && detail.layout != "records_nodes_split" {
        warn!(
            "node chunks present but layout is {}; skipping nodes chunk",
            detail.layout
        );
    }
    if !detail.nodes.is_empty() && detail.layout == "records_nodes_split" && !chunk_error {
        let mut nodes_by_record: HashMap<u64, Vec<Value>> = HashMap::new();
        for chunk in &detail.nodes {
            let mut last_record_index: Option<u64> = None;
            if !budget.consume_chunk() {
                budget_exceeded = true;
                chunk_error = true;
                break;
            }
            let lines = read_ndjson_chunk(base_dir, chunk, max_chunk_bytes, remaining_nodes)?;
            if lines.had_error {
                chunk_error = true;
            }
            if lines.size_exceeded {
                size_exceeded = true;
                chunk_error = true;
                break;
            }
            if lines.limit_exceeded {
                budget_exceeded = true;
                chunk_error = true;
                break;
            }
            if !budget.consume_bytes(lines.bytes) {
                budget_exceeded = true;
                chunk_error = true;
            }
            if !budget_exceeded {
                remaining_nodes = remaining_nodes.saturating_sub(lines.value.len());
            }
            for value in lines.value {
                let entry = parse_node_chunk_entry(value);
                if entry.record_index.is_none() && entry.record_index_present {
                    warn!("node chunk entry has invalid record_index; skipping");
                    continue;
                }
                let record_index = match entry.record_index.or(last_record_index) {
                    Some(index) => index,
                    None => {
                        warn!("node chunk entry missing record_index; skipping");
                        continue;
                    }
                };
                if entry.record_index.is_some() {
                    last_record_index = entry.record_index;
                }
                nodes_by_record
                    .entry(record_index)
                    .or_default()
                    .push(entry.node);
            }
            if budget_exceeded {
                break;
            }
        }
        let mut seen_record_indices: HashMap<u64, usize> = HashMap::new();
        let mut used_record_indices: HashMap<u64, usize> = HashMap::new();
        for (position, record) in records.iter_mut().enumerate() {
            let index_value = record.get("index");
            let parsed_index = index_value.and_then(parse_record_index);
            if parsed_index.is_none() && index_value.is_some() {
                warn!(
                    "invalid record_index in trace record; skipping node attach at position {}",
                    position
                );
                continue;
            }
            let record_index = parsed_index.unwrap_or(position as u64);
            if let Some(prev) = seen_record_indices.insert(record_index, position) {
                warn!(
                    "duplicate record_index in trace records: {} (at {} and {})",
                    record_index, prev, position
                );
            }
            let nodes_from_chunk = nodes_by_record.get(&record_index).cloned();
            if let Some(nodes) = nodes_from_chunk {
                if used_record_indices.contains_key(&record_index) {
                    warn!(
                        "node chunk entries already attached for record_index {}; skipping duplicate record",
                        record_index
                    );
                } else {
                    let mut attached = false;
                    if let Some(obj) = record.as_object_mut() {
                        match obj.get_mut("nodes") {
                            Some(existing) => {
                                if let Some(existing_nodes) = existing.as_array_mut() {
                                    if !nodes.is_empty() {
                                        warn!(
                                            "record has inline nodes and node chunk entries; merged record_index={}",
                                            record_index
                                        );
                                        existing_nodes.extend(nodes.clone());
                                        attached = true;
                                    }
                                } else {
                                    let previous =
                                        std::mem::replace(existing, Value::Array(Vec::new()));
                                    let mut combined = Vec::new();
                                    combined.push(previous);
                                    combined.extend(nodes.clone());
                                    *existing = Value::Array(combined);
                                    warn!(
                                        "record has non-array inline nodes and node chunk entries; merged record_index={}",
                                        record_index
                                    );
                                    attached = true;
                                }
                            }
                            None => {
                                obj.insert("nodes".to_string(), Value::Array(nodes.clone()));
                                attached = true;
                            }
                        }
                    } else {
                        warn!(
                            "record is non-object; skipping node attach for record_index={}",
                            record_index
                        );
                    }
                    if attached {
                        *used_record_indices.entry(record_index).or_insert(0) += 1;
                    }
                }
            }
        }
        if !nodes_by_record.is_empty() {
            let orphan_count: usize = nodes_by_record
                .iter()
                .filter(|(key, _)| !used_record_indices.contains_key(key))
                .map(|(_, nodes)| nodes.len())
                .sum();
            if orphan_count > 0 {
                warn!(
                    "node chunk entries not attached to records: {}",
                    orphan_count
                );
            }
        }
    }

    let finalize = if !chunk_error {
        match &detail.finalize {
            Some(chunk) => {
                if !budget.consume_chunk() {
                    budget_exceeded = true;
                    chunk_error = true;
                    None
                } else {
                    let result = read_json_chunk(base_dir, chunk, max_chunk_bytes)?;
                    if result.had_error {
                        chunk_error = true;
                    }
                    if result.size_exceeded {
                        size_exceeded = true;
                        chunk_error = true;
                        None
                    } else {
                        if !budget.consume_bytes(result.bytes) {
                            budget_exceeded = true;
                            chunk_error = true;
                            None
                        } else {
                            result.value
                        }
                    }
                }
            }
            None => None,
        }
    } else {
        None
    };

    if let Some(obj) = trace.as_object_mut() {
        obj.insert("records".to_string(), Value::Array(records));
        if let Some(finalize_value) = finalize {
            obj.insert("finalize".to_string(), finalize_value);
        }
        if chunk_error {
            if detail_status == "full" {
                detail_status = "basic".to_string();
            }
            if size_exceeded
                && !detail_reason
                    .iter()
                    .any(|reason| reason == "chunk_too_large")
            {
                detail_reason.push("chunk_too_large".to_string());
            }
            if budget_exceeded
                && !detail_reason
                    .iter()
                    .any(|reason| reason == "budget_exceeded")
            {
                detail_reason.push("budget_exceeded".to_string());
            }
            if !detail_reason.iter().any(|reason| reason == "chunk_error") {
                detail_reason.push("chunk_error".to_string());
            }
            obj.insert("records".to_string(), Value::Array(Vec::new()));
            obj.remove("finalize");
            if let Some(detail_obj) = obj
                .get_mut("detail")
                .and_then(|value| value.as_object_mut())
            {
                detail_obj.insert("status".to_string(), Value::String(detail_status));
                detail_obj.insert(
                    "reason".to_string(),
                    Value::Array(detail_reason.into_iter().map(Value::String).collect()),
                );
                detail_obj.insert("records".to_string(), Value::Array(Vec::new()));
                detail_obj.insert("nodes".to_string(), Value::Array(Vec::new()));
                detail_obj.remove("finalize");
            }
        }
    }

    Ok(trace)
}

async fn build_trace_from_manifest_async(
    manifest: TraceManifest,
    base_dir: PathBuf,
) -> Result<Value> {
    tokio::task::spawn_blocking(move || build_trace_from_manifest(&manifest, &base_dir))
        .await
        .map_err(|err| anyhow::anyhow!("trace load task failed: {}", err))?
}

fn is_trace_meta_candidate(path: &Path) -> bool {
    if path.extension().and_then(|s| s.to_str()) != Some("json") {
        return false;
    }
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "finalize.json")
    {
        return false;
    }
    if path
        .components()
        .any(|component| component.as_os_str() == "blobs")
    {
        return false;
    }
    true
}

// copy_dir_recursive was intentionally omitted to avoid counting existing files.
