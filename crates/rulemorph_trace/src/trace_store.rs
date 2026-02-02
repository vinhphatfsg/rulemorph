use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tracing::warn;
use walkdir::WalkDir;

use crate::trace_id::{sanitize_trace_id, trace_id_is_insufficient, trace_id_is_placeholder};
use crate::trace_schema::{RuleMeta, TraceChunkRef, TraceManifest, TraceSummary};

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
pub struct ImportResult {
    pub imported: usize,
    pub trace_ids: Vec<String>,
    pub rules_imported: usize,
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
        if !self.index.read().await.contains_key(trace_id) {
            self.refresh_index().await?;
        }
        let meta = match self.index.read().await.get(trace_id) {
            Some(meta) => meta.clone(),
            None => return Ok(None),
        };
        let path = PathBuf::from(&meta.path);
        let raw = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read trace: {}", path.display()))?;
        let value: Value = serde_json::from_str(&raw)
            .with_context(|| format!("invalid trace json: {}", path.display()))?;
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
            }
            Ok(Some(legacy))
        }
    }

    pub async fn seed_sample(&self) -> Result<()> {
        // No automatic sample seeding.
        self.refresh_index().await?;
        Ok(())
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub async fn import_bundle(&self, bundle_path: &Path) -> Result<ImportResult> {
        let traces_src = bundle_path.join("traces");
        let rules_src = bundle_path.join("rules");

        let mut imported = 0usize;
        let mut trace_ids = Vec::new();
        if traces_src.exists() {
            let dest = traces_dir(&self.data_dir);
            for entry in WalkDir::new(&traces_src).into_iter().filter_map(|e| e.ok()) {
                if entry.file_type().is_dir() {
                    continue;
                }
                let rel = entry.path().strip_prefix(&traces_src).unwrap();
                let target = dest.join(rel);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::copy(entry.path(), &target)?;
                if entry.path().extension().and_then(|s| s.to_str()) == Some("json") {
                    if let Ok(meta) = parse_trace_meta(entry.path()) {
                        imported += 1;
                        trace_ids.push(meta.trace_id);
                    }
                }
            }
        }

        let mut rules_imported = 0usize;
        if rules_src.exists() {
            let dest = rules_dir(&self.data_dir);
            for entry in WalkDir::new(&rules_src).into_iter().filter_map(|e| e.ok()) {
                let rel = entry.path().strip_prefix(&rules_src).unwrap();
                let target = dest.join(rel);
                if entry.file_type().is_dir() {
                    std::fs::create_dir_all(&target)?;
                    continue;
                }
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::copy(entry.path(), &target)?;
                rules_imported += 1;
            }
        }

        self.refresh_index().await?;

        Ok(ImportResult {
            imported,
            trace_ids,
            rules_imported,
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
            for entry in WalkDir::new(&dir).into_iter().filter_map(|e| e.ok()) {
                if !entry.file_type().is_file() {
                    continue;
                }
                if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }
                if let Ok(meta) = parse_trace_meta(entry.path()) {
                    metas.push(meta);
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
    use super::{fallback_trace_id_for_path, path_hash_for_trace_id};
    use crate::trace_id::sanitize_trace_id;
    use std::path::Path;

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
}

fn parse_trace_meta(path: &Path) -> Result<TraceMeta> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read trace: {}", path.display()))?;
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
    let mut trace = serde_json::to_value(manifest)?;
    let mut records = Vec::new();

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
        }
        return Ok(trace);
    }

    for chunk in &detail.records {
        let lines = read_ndjson_chunk(base_dir, chunk)?;
        records.extend(lines);
    }

    for record in &mut records {
        if let Some(obj) = record.as_object_mut() {
            if let Some(nodes_value) = obj.get("nodes").cloned() {
                let normalized = normalize_inline_nodes_value(&nodes_value);
                obj.insert("nodes".to_string(), Value::Array(normalized));
            }
        }
    }

    if !detail.nodes.is_empty() && detail.layout != "records_nodes_split" {
        warn!(
            "node chunks present but layout is {}; skipping nodes chunk",
            detail.layout
        );
    }
    if !detail.nodes.is_empty() && detail.layout == "records_nodes_split" {
        let mut nodes_by_record: HashMap<u64, Vec<Value>> = HashMap::new();
        for chunk in &detail.nodes {
            let mut last_record_index: Option<u64> = None;
            let lines = read_ndjson_chunk(base_dir, chunk)?;
            for value in lines {
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

    let finalize = match &detail.finalize {
        Some(chunk) => read_json_chunk(base_dir, chunk)?,
        None => None,
    };

    if let Some(obj) = trace.as_object_mut() {
        obj.insert("records".to_string(), Value::Array(records));
        if let Some(finalize_value) = finalize {
            obj.insert("finalize".to_string(), finalize_value);
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

fn parse_record_index(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn read_ndjson_chunk(base_dir: &Path, chunk: &TraceChunkRef) -> Result<Vec<Value>> {
    if chunk.format != "ndjson" {
        warn!(
            "unsupported chunk format {}; skipping chunk {}",
            chunk.format, chunk.path
        );
        return Ok(Vec::new());
    }
    if !is_supported_compression(&chunk.compression) {
        warn!(
            "unsupported chunk compression {}; skipping chunk {}",
            chunk.compression, chunk.path
        );
        return Ok(Vec::new());
    }
    let chunk_path = base_dir.join(&chunk.path);
    let raw = match read_chunk_bytes(base_dir, chunk) {
        Ok(raw) => raw,
        Err(err) => {
            warn!(
                "failed to read/decode ndjson chunk {}; skipping chunk: {}",
                chunk.path, err
            );
            return Ok(Vec::new());
        }
    };
    let text = match String::from_utf8(raw) {
        Ok(text) => text,
        Err(err) => {
            warn!(
                "failed to decode ndjson chunk as utf-8 {}; skipping chunk {}",
                err, chunk.path
            );
            return Ok(Vec::new());
        }
    };
    let mut values = Vec::new();
    for (line_number, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
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
            }
        }
    }
    Ok(values)
}

fn read_json_chunk(base_dir: &Path, chunk: &TraceChunkRef) -> Result<Option<Value>> {
    if chunk.format != "json" {
        warn!(
            "unsupported chunk format {}; skipping chunk {}",
            chunk.format, chunk.path
        );
        return Ok(None);
    }
    if !is_supported_compression(&chunk.compression) {
        warn!(
            "unsupported chunk compression {}; skipping chunk {}",
            chunk.compression, chunk.path
        );
        return Ok(None);
    }
    let raw = match read_chunk_bytes(base_dir, chunk) {
        Ok(raw) => raw,
        Err(err) => {
            warn!(
                "failed to read/decode json chunk {}; skipping chunk: {}",
                chunk.path, err
            );
            return Ok(None);
        }
    };
    let value = match serde_json::from_slice(&raw) {
        Ok(value) => value,
        Err(err) => {
            warn!(
                "failed to parse json chunk {}; skipping chunk: {}",
                chunk.path, err
            );
            return Ok(None);
        }
    };
    Ok(Some(value))
}

fn is_supported_compression(compression: &str) -> bool {
    matches!(compression, "zstd" | "none")
}

fn read_chunk_bytes(base_dir: &Path, chunk: &TraceChunkRef) -> Result<Vec<u8>> {
    let path = base_dir.join(&chunk.path);
    let raw = std::fs::read(&path)
        .with_context(|| format!("failed to read trace chunk: {}", path.display()))?;
    match chunk.compression.as_str() {
        "zstd" => Ok(zstd::stream::decode_all(raw.as_slice())?),
        "none" => Ok(raw),
        other => Err(anyhow::anyhow!("unsupported compression: {}", other)),
    }
}

// copy_dir_recursive was intentionally omitted to avoid counting existing files.
