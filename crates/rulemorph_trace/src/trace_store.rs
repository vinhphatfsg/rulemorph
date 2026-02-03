use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tracing::warn;
use walkdir::WalkDir;

use crate::trace_id::{sanitize_trace_id, trace_id_is_insufficient, trace_id_is_placeholder};
use crate::trace_schema::{
    RuleMeta, TRACE_CHUNK_BYTES_COMPRESSED_HARD_MAX, TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX,
    TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TRACE_CHUNK_COUNT_HARD_MAX, TRACE_JSON_MAX_BYTES,
    TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX,
    TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TraceChunkRef, TraceDetailRef, TraceManifest,
    TraceSummary,
};

const HARD_MAX_CHUNK_BYTES_COMPRESSED: u64 = TRACE_CHUNK_BYTES_COMPRESSED_HARD_MAX as u64;
const IMPORT_MAX_FILE_BYTES: u64 = 20 * 1024 * 1024;
const IMPORT_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
// Zstd frames produced with default settings expect at least an 8MB window.
const ZSTD_WINDOW_BYTES_MIN: usize = 8 * 1024 * 1024;

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

struct ImportWorkResult {
    imported_paths: Vec<PathBuf>,
    rules_imported: usize,
}

#[derive(Debug, Clone)]
pub struct TraceStore {
    data_dir: PathBuf,
    index: Arc<RwLock<HashMap<String, TraceMeta>>>,
}

impl TraceStore {
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
            let manifest: TraceManifest = serde_json::from_value(value.clone())
                .with_context(|| format!("invalid trace manifest: {}", path.display()))?;
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

    pub async fn seed_sample(&self) -> Result<()> {
        // No automatic sample seeding.
        self.refresh_index().await?;
        Ok(())
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
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
        let result = tokio::task::spawn_blocking(move || -> Result<ImportWorkResult> {
            let traces_src = bundle_path.join("traces");
            let rules_src = bundle_path.join("rules");

            let mut copied_paths = Vec::new();
            let mut created_dirs = BTreeSet::new();

            let work = (|| -> Result<ImportWorkResult> {
                let mut imported_paths = Vec::new();
                let mut total_bytes: u64 = 0;
                if traces_src.exists() {
                    let dest = traces_dir(&data_dir);
                    let dest_canon = ensure_import_base_dir(&dest)?;
                    let mut entries: Vec<(PathBuf, PathBuf)> = Vec::new();
                    for entry in WalkDir::new(&traces_src)
                        .follow_links(false)
                        .into_iter()
                        .filter_map(|e| e.ok())
                    {
                        if entry.file_type().is_symlink() {
                            return Err(anyhow::anyhow!(
                                "bundle contains symlink: {}",
                                entry.path().display()
                            ));
                        }
                        if entry.file_type().is_file() {
                            let rel = entry.path().strip_prefix(&traces_src).unwrap();
                            let target = dest.join(rel);
                            let file_bytes = entry.metadata().map(|meta| meta.len())?;
                            if file_bytes > IMPORT_MAX_FILE_BYTES {
                                return Err(anyhow::anyhow!(
                                    "bundle file exceeds max bytes: {} > {} ({})",
                                    file_bytes,
                                    IMPORT_MAX_FILE_BYTES,
                                    entry.path().display()
                                ));
                            }
                            total_bytes = total_bytes.saturating_add(file_bytes);
                            if total_bytes > max_total_bytes {
                                return Err(anyhow::anyhow!(
                                    "bundle exceeds max total bytes: {} > {}",
                                    total_bytes,
                                    max_total_bytes
                                ));
                            }
                            entries.push((entry.path().to_path_buf(), target));
                        }
                    }
                    for (_, target) in &entries {
                        if target.exists() {
                            return Err(anyhow::anyhow!(
                                "bundle would overwrite existing file: {}",
                                target.display()
                            ));
                        }
                    }
                    for (source, target) in entries {
                        ensure_import_target_parent(
                            &dest,
                            &dest_canon,
                            &target,
                            &mut created_dirs,
                        )?;
                        copy_file_create_new(&source, &target, &dest_canon)?;
                        copied_paths.push(target.clone());
                        if is_trace_meta_candidate(&target) {
                            if parse_trace_meta(&target).is_ok() {
                                imported_paths.push(target);
                            }
                        }
                    }
                }

                let mut rules_imported = 0usize;
                if rules_src.exists() {
                    let dest = rules_dir(&data_dir);
                    let dest_canon = ensure_import_base_dir(&dest)?;
                    let mut entries: Vec<(PathBuf, PathBuf)> = Vec::new();
                    for entry in WalkDir::new(&rules_src)
                        .follow_links(false)
                        .into_iter()
                        .filter_map(|e| e.ok())
                    {
                        if entry.file_type().is_symlink() {
                            return Err(anyhow::anyhow!(
                                "bundle contains symlink: {}",
                                entry.path().display()
                            ));
                        }
                        if entry.file_type().is_file() {
                            let rel = entry.path().strip_prefix(&rules_src).unwrap();
                            let target = dest.join(rel);
                            let file_bytes = entry.metadata().map(|meta| meta.len())?;
                            if file_bytes > IMPORT_MAX_FILE_BYTES {
                                return Err(anyhow::anyhow!(
                                    "bundle file exceeds max bytes: {} > {} ({})",
                                    file_bytes,
                                    IMPORT_MAX_FILE_BYTES,
                                    entry.path().display()
                                ));
                            }
                            total_bytes = total_bytes.saturating_add(file_bytes);
                            if total_bytes > max_total_bytes {
                                return Err(anyhow::anyhow!(
                                    "bundle exceeds max total bytes: {} > {}",
                                    total_bytes,
                                    max_total_bytes
                                ));
                            }
                            entries.push((entry.path().to_path_buf(), target));
                        }
                    }
                    for (_, target) in &entries {
                        if target.exists() {
                            return Err(anyhow::anyhow!(
                                "bundle would overwrite existing file: {}",
                                target.display()
                            ));
                        }
                    }
                    for (source, target) in entries {
                        ensure_import_target_parent(
                            &dest,
                            &dest_canon,
                            &target,
                            &mut created_dirs,
                        )?;
                        copy_file_create_new(&source, &target, &dest_canon)?;
                        copied_paths.push(target);
                        rules_imported += 1;
                    }
                }

                Ok(ImportWorkResult {
                    imported_paths,
                    rules_imported,
                })
            })();

            if let Err(err) = work {
                rollback_import(&copied_paths, &created_dirs);
                return Err(err);
            }

            work
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

fn traces_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("traces")
}

fn rules_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("rules")
}

fn ensure_import_base_dir(base_dir: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(base_dir)
        .with_context(|| format!("failed to create import base dir: {}", base_dir.display()))?;
    let meta = std::fs::symlink_metadata(base_dir)
        .with_context(|| format!("failed to stat import base dir: {}", base_dir.display()))?;
    if meta.file_type().is_symlink() {
        return Err(anyhow::anyhow!(
            "bundle destination base is symlink: {}",
            base_dir.display()
        ));
    }
    if !meta.is_dir() {
        return Err(anyhow::anyhow!(
            "bundle destination base is not a directory: {}",
            base_dir.display()
        ));
    }
    base_dir.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize import base dir: {}",
            base_dir.display()
        )
    })
}

fn ensure_relative_dir_safe(
    base_dir: &Path,
    target_dir: &Path,
    created_dirs: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    let relative = target_dir.strip_prefix(base_dir).with_context(|| {
        format!(
            "bundle destination escapes base dir: {}",
            target_dir.display()
        )
    })?;
    let mut current = base_dir.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match std::fs::create_dir(&current) {
            Ok(()) => {
                created_dirs.insert(current.clone());
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let meta = std::fs::symlink_metadata(&current).with_context(|| {
                    format!("failed to stat bundle destination: {}", current.display())
                })?;
                if meta.file_type().is_symlink() {
                    return Err(anyhow::anyhow!(
                        "bundle destination contains symlink: {}",
                        current.display()
                    ));
                }
                if !meta.is_dir() {
                    return Err(anyhow::anyhow!(
                        "bundle destination is not a directory: {}",
                        current.display()
                    ));
                }
            }
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "failed to create bundle destination dir {}: {}",
                    current.display(),
                    err
                ));
            }
        }
    }
    Ok(())
}

fn ensure_import_target_parent(
    base_dir: &Path,
    base_canon: &Path,
    target: &Path,
    created_dirs: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    if !target.starts_with(base_dir) {
        return Err(anyhow::anyhow!(
            "bundle destination escapes base dir: {}",
            target.display()
        ));
    }
    let parent = target
        .parent()
        .ok_or_else(|| anyhow::anyhow!("bundle destination has no parent: {}", target.display()))?;
    ensure_relative_dir_safe(base_dir, parent, created_dirs)?;
    let parent_canon = parent.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize bundle destination parent: {}",
            parent.display()
        )
    })?;
    if !parent_canon.starts_with(base_canon) {
        return Err(anyhow::anyhow!(
            "bundle destination escapes base dir: {}",
            target.display()
        ));
    }
    Ok(())
}

fn copy_file_create_new(source: &Path, target: &Path, base_canon: &Path) -> Result<()> {
    let mut source_file = std::fs::File::open(source)
        .with_context(|| format!("failed to open bundle source: {}", source.display()))?;
    let mut target_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)
        .with_context(|| format!("failed to create bundle target: {}", target.display()))?;
    let target_canon = target
        .canonicalize()
        .with_context(|| format!("failed to canonicalize bundle target: {}", target.display()))?;
    if !target_canon.starts_with(base_canon) {
        drop(target_file);
        let _ = std::fs::remove_file(target);
        return Err(anyhow::anyhow!(
            "bundle target escapes base dir: {}",
            target.display()
        ));
    }
    std::io::copy(&mut source_file, &mut target_file).with_context(|| {
        format!(
            "failed to copy bundle file {} -> {}",
            source.display(),
            target.display()
        )
    })?;
    Ok(())
}

fn rollback_import(copied_paths: &[PathBuf], created_dirs: &BTreeSet<PathBuf>) {
    for path in copied_paths.iter().rev() {
        if let Err(err) = std::fs::remove_file(path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    "failed to rollback imported file {}: {}",
                    path.display(),
                    err
                );
            }
        }
    }

    let mut dirs: Vec<&PathBuf> = created_dirs.iter().collect();
    dirs.sort_by_key(|path| Reverse(path.components().count()));
    for dir in dirs {
        if let Err(err) = std::fs::remove_dir(dir) {
            if err.kind() != std::io::ErrorKind::NotFound {
                warn!("failed to rollback import dir {}: {}", dir.display(), err);
            }
        }
    }
}

fn fnv1a_hash(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn relative_trace_path(path: &Path) -> Option<PathBuf> {
    for ancestor in path.ancestors() {
        if ancestor.file_name().and_then(|name| name.to_str()) == Some("traces") {
            if let Ok(rel) = path.strip_prefix(ancestor) {
                if !rel.as_os_str().is_empty() {
                    return Some(rel.to_path_buf());
                }
            }
        }
    }
    None
}

#[cfg(unix)]
fn path_bytes_for_hash(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    let candidate = relative_trace_path(path).unwrap_or_else(|| path.to_path_buf());
    candidate.as_os_str().as_bytes().to_vec()
}

#[cfg(not(unix))]
fn path_bytes_for_hash(path: &Path) -> Vec<u8> {
    let candidate = relative_trace_path(path).unwrap_or_else(|| path.to_path_buf());
    candidate.to_string_lossy().into_owned().into_bytes()
}

fn path_hash_for_trace_id(path: &Path) -> u64 {
    fnv1a_hash(&path_bytes_for_hash(path))
}

fn hash_trace_id_for_path(path: &Path) -> String {
    let hash = path_hash_for_trace_id(path);
    format!("trace-{hash:x}")
}

fn fallback_trace_id_for_path(path: &Path) -> String {
    let stem = path.file_stem().and_then(|s| s.to_str());
    let sanitized = stem.map(sanitize_trace_id).unwrap_or_default();
    if !trace_id_is_insufficient(&sanitized) {
        return sanitized;
    }
    hash_trace_id_for_path(path)
}

#[cfg(test)]
mod tests {
    use super::{
        ChunkBudget, Result, TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TRACE_CHUNK_COUNT_HARD_MAX,
        TRACE_NODE_COUNT_HARD_MAX, TRACE_RECORD_COUNT_HARD_MAX,
        TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TraceChunkRef, TraceManifest, TraceStore,
        apply_legacy_limits_with_thresholds, build_trace_from_manifest_with_budget,
        fallback_trace_id_for_path, path_hash_for_trace_id, resolve_chunk_path,
    };
    use crate::TraceDetailRef;
    use crate::trace_id::sanitize_trace_id;
    use serde_json::{Value, json};
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;

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
                            "compression": "zstd"
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
        let store = TraceStore::new(data_dir).await?;
        let err = store
            .import_bundle_inner(&bundle_dir, max_total_bytes)
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

fn resolve_max_chunk_bytes(manifest: &TraceManifest) -> usize {
    let requested = manifest
        .max_chunk_bytes_uncompressed
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX);
    requested.clamp(1, TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX)
}

fn estimate_chunk_item_count(
    chunks: &[TraceChunkRef],
    start: impl Fn(&TraceChunkRef) -> Option<u64>,
    end: impl Fn(&TraceChunkRef) -> Option<u64>,
) -> Option<u64> {
    let mut total = 0u64;
    for chunk in chunks {
        let start = start(chunk)?;
        let end = end(chunk)?;
        if end < start {
            return None;
        }
        total = total.checked_add(end - start + 1)?;
    }
    Some(total)
}

fn estimate_uncompressed_bytes(
    detail: &TraceDetailRef,
    max_chunk_bytes: Option<u64>,
) -> Option<u64> {
    let mut total = 0u64;
    let mut add_chunk = |chunk: &TraceChunkRef| -> Option<()> {
        let bytes = match chunk.bytes_uncompressed {
            Some(bytes) => bytes,
            None if chunk.compression == "none" => chunk
                .bytes
                .or(max_chunk_bytes)
                .unwrap_or(TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX as u64),
            None => {
                if let Some(max_chunk_bytes) = max_chunk_bytes {
                    max_chunk_bytes
                } else {
                    chunk.bytes?
                }
            }
        };
        total = total.checked_add(bytes)?;
        Some(())
    };
    for chunk in detail.records.iter().chain(detail.nodes.iter()) {
        add_chunk(chunk)?;
    }
    if let Some(chunk) = detail.finalize.as_ref() {
        add_chunk(chunk)?;
    }
    Some(total)
}

fn apply_manifest_budget(manifest: &mut TraceManifest) {
    let summary_record_total = manifest
        .summary
        .as_ref()
        .and_then(|summary| summary.record_total);
    let Some(detail) = manifest.detail.as_mut() else {
        return;
    };
    if detail.status != "full" {
        return;
    }

    let mut budget_exceeded = false;

    let chunk_count =
        detail.records.len() + detail.nodes.len() + usize::from(detail.finalize.is_some());
    if chunk_count > TRACE_CHUNK_COUNT_HARD_MAX {
        budget_exceeded = true;
    }

    let max_chunk_bytes = manifest
        .max_chunk_bytes_uncompressed
        .and_then(|value| usize::try_from(value).ok())
        .map(|value| value.clamp(1, TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX) as u64);

    match estimate_uncompressed_bytes(detail, max_chunk_bytes) {
        Some(estimated_bytes) => {
            if estimated_bytes > TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX as u64 {
                budget_exceeded = true;
            }
        }
        None => {
            budget_exceeded = true;
        }
    }

    let record_total = if detail.records.is_empty() {
        summary_record_total
    } else {
        match estimate_chunk_item_count(
            &detail.records,
            |chunk| chunk.record_start,
            |chunk| chunk.record_end,
        ) {
            Some(record_total) => Some(record_total),
            None => summary_record_total,
        }
    };
    if let Some(record_total) = record_total {
        if record_total as usize > TRACE_RECORD_COUNT_HARD_MAX {
            budget_exceeded = true;
        }
    } else if !detail.records.is_empty() {
        budget_exceeded = true;
    }

    let node_total = if detail.nodes.is_empty() {
        if detail.layout == "records_inline" {
            record_total
        } else {
            None
        }
    } else {
        estimate_chunk_item_count(
            &detail.nodes,
            |chunk| chunk.node_start,
            |chunk| chunk.node_end,
        )
    };
    if let Some(node_total) = node_total {
        if node_total as usize > TRACE_NODE_COUNT_HARD_MAX {
            budget_exceeded = true;
        }
    } else if !detail.nodes.is_empty() {
        budget_exceeded = true;
    }

    if budget_exceeded {
        detail.status = "basic".to_string();
        if !detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
        {
            detail.reason.push("budget_exceeded".to_string());
        }
        detail.records.clear();
        detail.nodes.clear();
        detail.finalize = None;
    }
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

struct NodeChunkEntry {
    record_index: Option<u64>,
    record_index_present: bool,
    node: Value,
}

fn parse_node_chunk_entry(value: Value) -> NodeChunkEntry {
    let record_index_value = value.get("record_index");
    let record_index_present = record_index_value.is_some();
    let record_index = record_index_value.and_then(parse_record_index);
    let (has_node_wrapper, extra_keys) = match value.as_object() {
        Some(obj) => {
            let has_node = obj.contains_key("node");
            let has_core_fields =
                obj.contains_key("id") || obj.contains_key("kind") || obj.contains_key("status");
            let extras = obj
                .keys()
                .filter(|key| key.as_str() != "node" && key.as_str() != "record_index")
                .cloned()
                .collect::<Vec<_>>();
            let is_wrapper = has_node && !has_core_fields;
            (is_wrapper, extras)
        }
        None => (false, Vec::new()),
    };
    if has_node_wrapper && !extra_keys.is_empty() {
        warn!("node wrapper has extra keys ignored: {:?}", extra_keys);
    }
    let mut node = if has_node_wrapper {
        value.get("node").cloned().unwrap_or(Value::Null)
    } else {
        let mut node = value;
        if let Some(obj) = node.as_object_mut() {
            obj.remove("record_index");
        }
        node
    };
    if !node.is_object() {
        node = json!({ "value": node });
    }
    NodeChunkEntry {
        record_index,
        record_index_present,
        node,
    }
}

fn normalize_inline_nodes_value(nodes_value: &Value) -> Vec<Value> {
    match nodes_value {
        Value::Array(nodes) => nodes.iter().map(normalize_inline_node).collect(),
        Value::Object(_) => vec![normalize_inline_node(nodes_value)],
        other => vec![json!({ "value": other })],
    }
}

fn normalize_inline_node(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(map.clone()),
        other => json!({ "value": other }),
    }
}

fn count_inline_nodes(records: &[Value], max_nodes: usize) -> usize {
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

fn parse_record_index(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

struct ChunkReadResult<T> {
    value: T,
    had_error: bool,
    size_exceeded: bool,
    limit_exceeded: bool,
    bytes: usize,
}

struct ChunkBudget {
    remaining_bytes: usize,
    remaining_chunks: usize,
}

impl ChunkBudget {
    fn new() -> Self {
        Self {
            remaining_bytes: TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX,
            remaining_chunks: TRACE_CHUNK_COUNT_HARD_MAX,
        }
    }

    fn consume_chunk(&mut self) -> bool {
        if self.remaining_chunks == 0 {
            return false;
        }
        self.remaining_chunks = self.remaining_chunks.saturating_sub(1);
        true
    }

    fn consume_bytes(&mut self, bytes: usize) -> bool {
        if bytes > self.remaining_bytes {
            return false;
        }
        self.remaining_bytes = self.remaining_bytes.saturating_sub(bytes);
        true
    }
}

fn read_ndjson_chunk(
    base_dir: &Path,
    chunk: &TraceChunkRef,
    max_bytes: usize,
    max_items: usize,
) -> Result<ChunkReadResult<Vec<Value>>> {
    if max_items == 0 {
        warn!(
            "trace chunk exceeds max item count; skipping chunk {}",
            chunk.path
        );
        return Ok(ChunkReadResult {
            value: Vec::new(),
            had_error: true,
            size_exceeded: false,
            limit_exceeded: true,
            bytes: 0,
        });
    }
    if chunk.format != "ndjson" {
        warn!(
            "unsupported chunk format {}; skipping chunk {}",
            chunk.format, chunk.path
        );
        return Ok(ChunkReadResult {
            value: Vec::new(),
            had_error: true,
            size_exceeded: false,
            limit_exceeded: false,
            bytes: 0,
        });
    }
    if !is_supported_compression(&chunk.compression) {
        warn!(
            "unsupported chunk compression {}; skipping chunk {}",
            chunk.compression, chunk.path
        );
        return Ok(ChunkReadResult {
            value: Vec::new(),
            had_error: true,
            size_exceeded: false,
            limit_exceeded: false,
            bytes: 0,
        });
    }
    let chunk_path = base_dir.join(&chunk.path);
    let raw = match read_chunk_bytes(base_dir, chunk, max_bytes) {
        Ok(raw) => raw,
        Err(err) => {
            let size_exceeded = err.downcast_ref::<ChunkSizeExceeded>().is_some();
            warn!(
                "failed to read/decode ndjson chunk {}; skipping chunk: {}",
                chunk.path, err
            );
            return Ok(ChunkReadResult {
                value: Vec::new(),
                had_error: true,
                size_exceeded,
                limit_exceeded: false,
                bytes: 0,
            });
        }
    };
    let raw_len = raw.len();
    let text = match String::from_utf8(raw) {
        Ok(text) => text,
        Err(err) => {
            warn!(
                "failed to decode ndjson chunk as utf-8 {}; skipping chunk {}",
                err, chunk.path
            );
            return Ok(ChunkReadResult {
                value: Vec::new(),
                had_error: true,
                size_exceeded: false,
                limit_exceeded: false,
                bytes: 0,
            });
        }
    };
    let mut values = Vec::new();
    let mut had_error = false;
    let mut limit_exceeded = false;
    for (line_number, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if values.len() >= max_items {
            warn!(
                "ndjson chunk exceeds max item count {}; stopping at {} for {}",
                max_items,
                values.len(),
                chunk_path.display()
            );
            had_error = true;
            limit_exceeded = true;
            break;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => values.push(value),
            Err(err) => {
                warn!(
                    "skipping malformed ndjson line {} in {}: {}",
                    line_number + 1,
                    chunk_path.display(),
                    err
                );
                had_error = true;
            }
        }
    }
    Ok(ChunkReadResult {
        value: values,
        had_error,
        size_exceeded: false,
        limit_exceeded,
        bytes: raw_len,
    })
}

fn read_json_chunk(
    base_dir: &Path,
    chunk: &TraceChunkRef,
    max_bytes: usize,
) -> Result<ChunkReadResult<Option<Value>>> {
    if chunk.format != "json" {
        warn!(
            "unsupported chunk format {}; skipping chunk {}",
            chunk.format, chunk.path
        );
        return Ok(ChunkReadResult {
            value: None,
            had_error: true,
            size_exceeded: false,
            limit_exceeded: false,
            bytes: 0,
        });
    }
    if !is_supported_compression(&chunk.compression) {
        warn!(
            "unsupported chunk compression {}; skipping chunk {}",
            chunk.compression, chunk.path
        );
        return Ok(ChunkReadResult {
            value: None,
            had_error: true,
            size_exceeded: false,
            limit_exceeded: false,
            bytes: 0,
        });
    }
    let raw = match read_chunk_bytes(base_dir, chunk, max_bytes) {
        Ok(raw) => raw,
        Err(err) => {
            let size_exceeded = err.downcast_ref::<ChunkSizeExceeded>().is_some();
            warn!(
                "failed to read/decode json chunk {}; skipping chunk: {}",
                chunk.path, err
            );
            return Ok(ChunkReadResult {
                value: None,
                had_error: true,
                size_exceeded,
                limit_exceeded: false,
                bytes: 0,
            });
        }
    };
    let raw_len = raw.len();
    let value = match serde_json::from_slice(&raw) {
        Ok(value) => value,
        Err(err) => {
            warn!(
                "failed to parse json chunk {}; skipping chunk: {}",
                chunk.path, err
            );
            return Ok(ChunkReadResult {
                value: None,
                had_error: true,
                size_exceeded: false,
                limit_exceeded: false,
                bytes: 0,
            });
        }
    };
    Ok(ChunkReadResult {
        value: Some(value),
        had_error: false,
        size_exceeded: false,
        limit_exceeded: false,
        bytes: raw_len,
    })
}

fn is_supported_compression(compression: &str) -> bool {
    matches!(compression, "zstd" | "none")
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

#[derive(Debug)]
struct ChunkSizeExceeded {
    actual: u64,
    max: u64,
}

impl fmt::Display for ChunkSizeExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "trace chunk exceeds max bytes: {} > {}",
            self.actual, self.max
        )
    }
}

impl std::error::Error for ChunkSizeExceeded {}

fn resolve_chunk_path(base_dir: &Path, chunk_path: &str) -> Result<PathBuf> {
    let rel = Path::new(chunk_path);
    if rel.as_os_str().is_empty() {
        return Err(anyhow::anyhow!("trace chunk path is empty"));
    }
    if rel.is_absolute()
        || rel.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(anyhow::anyhow!(
            "trace chunk path must be relative without parent components: {}",
            chunk_path
        ));
    }

    let base_dir = base_dir.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize trace base dir: {}",
            base_dir.display()
        )
    })?;
    let path = base_dir.join(rel);
    let resolved = path.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize trace chunk path: {}",
            path.display()
        )
    })?;
    if !resolved.starts_with(&base_dir) {
        return Err(anyhow::anyhow!(
            "trace chunk path escapes base dir: {}",
            chunk_path
        ));
    }
    Ok(resolved)
}

fn read_chunk_bytes(base_dir: &Path, chunk: &TraceChunkRef, max_bytes: usize) -> Result<Vec<u8>> {
    let path = resolve_chunk_path(base_dir, &chunk.path)?;
    let compressed_bytes = std::fs::metadata(&path)
        .with_context(|| format!("failed to read trace chunk metadata: {}", path.display()))?
        .len();
    let max_compressed_bytes = std::cmp::min(
        HARD_MAX_CHUNK_BYTES_COMPRESSED,
        (max_bytes as u64).saturating_add(TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX as u64),
    );
    if compressed_bytes > max_compressed_bytes {
        return Err(anyhow::Error::new(ChunkSizeExceeded {
            actual: compressed_bytes,
            max: max_compressed_bytes,
        }));
    }
    let raw = std::fs::read(&path)
        .with_context(|| format!("failed to read trace chunk: {}", path.display()))?;
    match chunk.compression.as_str() {
        "zstd" => decode_zstd_limited(&raw, max_bytes),
        "none" => {
            if raw.len() > max_bytes {
                return Err(anyhow::Error::new(ChunkSizeExceeded {
                    actual: raw.len() as u64,
                    max: max_bytes as u64,
                }));
            }
            Ok(raw)
        }
        other => Err(anyhow::anyhow!("unsupported compression: {}", other)),
    }
}

fn decode_zstd_limited(raw: &[u8], max_bytes: usize) -> Result<Vec<u8>> {
    let mut decoder = zstd::stream::read::Decoder::new(raw)?;
    decoder.window_log_max(zstd_window_log_max(max_bytes))?;
    let mut limited = decoder.take((max_bytes as u64).saturating_add(1));
    let mut output = Vec::new();
    limited.read_to_end(&mut output)?;
    if output.len() > max_bytes {
        return Err(anyhow::Error::new(ChunkSizeExceeded {
            actual: output.len() as u64,
            max: max_bytes as u64,
        }));
    }
    Ok(output)
}

fn zstd_window_log_max(max_bytes: usize) -> u32 {
    let max_bytes = max_bytes
        .max(ZSTD_WINDOW_BYTES_MIN)
        .min(TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX)
        .max(1) as u64;
    let pow2 = max_bytes.next_power_of_two();
    let log = 63u32.saturating_sub(pow2.leading_zeros());
    log.clamp(20, 31)
}

// copy_dir_recursive was intentionally omitted to avoid counting existing files.
