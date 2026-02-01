use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;
use walkdir::WalkDir;

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
        let path = match self.index.read().await.get(trace_id) {
            Some(meta) => PathBuf::from(&meta.path),
            None => return Ok(None),
        };
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
            let full = build_trace_from_manifest_async(manifest, trace_dir).await?;
            Ok(Some(full))
        } else {
            Ok(Some(value))
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
            let mut map = HashMap::new();
            let dir = traces_dir(&data_dir);
            if !dir.exists() {
                return Ok(map);
            }
            for entry in WalkDir::new(&dir).into_iter().filter_map(|e| e.ok()) {
                if !entry.file_type().is_file() {
                    continue;
                }
                if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }
                if let Ok(meta) = parse_trace_meta(entry.path()) {
                    map.insert(meta.trace_id.clone(), meta);
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

    let trace_id = value
        .get("trace_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string()
        });

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

    Ok(TraceMeta {
        trace_id: manifest.trace_id.clone(),
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

    if !detail.nodes.is_empty() && detail.layout == "records_nodes_split" {
        let mut nodes_by_record: HashMap<u64, Vec<Value>> = HashMap::new();
        for chunk in &detail.nodes {
            let lines = read_ndjson_chunk(base_dir, chunk)?;
            for value in lines {
                let (record_index, node) = parse_node_chunk_entry(value);
                nodes_by_record.entry(record_index).or_default().push(node);
            }
        }
        for record in &mut records {
            if record.get("nodes").is_some() {
                continue;
            }
            let record_index = record.get("index").and_then(|v| v.as_u64()).unwrap_or(0);
            if let Some(nodes) = nodes_by_record.remove(&record_index) {
                if let Some(obj) = record.as_object_mut() {
                    obj.insert("nodes".to_string(), Value::Array(nodes));
                }
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

fn parse_node_chunk_entry(value: Value) -> (u64, Value) {
    let record_index = value
        .get("record_index")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    if let Some(node) = value.get("node") {
        return (record_index, node.clone());
    }

    let mut node = value;
    if let Some(obj) = node.as_object_mut() {
        obj.remove("record_index");
    }
    (record_index, node)
}

fn read_ndjson_chunk(base_dir: &Path, chunk: &TraceChunkRef) -> Result<Vec<Value>> {
    if chunk.format != "ndjson" {
        return Err(anyhow::anyhow!(
            "unsupported chunk format: {}",
            chunk.format
        ));
    }
    let raw = read_chunk_bytes(base_dir, chunk)?;
    let text = String::from_utf8(raw)?;
    let mut values = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line)?;
        values.push(value);
    }
    Ok(values)
}

fn read_json_chunk(base_dir: &Path, chunk: &TraceChunkRef) -> Result<Option<Value>> {
    if chunk.format != "json" {
        return Err(anyhow::anyhow!(
            "unsupported chunk format: {}",
            chunk.format
        ));
    }
    let raw = read_chunk_bytes(base_dir, chunk)?;
    let value: Value = serde_json::from_slice(&raw)?;
    Ok(Some(value))
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
