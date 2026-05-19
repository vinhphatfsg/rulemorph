use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;
use tracing::warn;

use super::TraceMeta;
use super::legacy::looks_like_legacy_trace;
use super::trace_id_path::{fallback_trace_id_for_path, hash_trace_id_for_path};
use crate::trace_id::{sanitize_trace_id, trace_id_is_placeholder};
use crate::trace_schema::{RuleMeta, TRACE_JSON_MAX_BYTES, TraceManifest, TraceSummary};

pub(super) async fn read_trace_json_with_limit_async(path: &Path) -> Result<String> {
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

pub(super) async fn read_trace_value_with_limit_async(path: &Path) -> Result<Value> {
    let raw = read_trace_json_with_limit_async(path).await?;
    let parse_result = tokio::task::spawn_blocking({
        let raw = raw.clone();
        move || serde_json::from_str::<Value>(&raw)
    })
    .await
    .map_err(|err| anyhow::anyhow!("trace json parse task failed: {}", err))?;
    parse_result.with_context(|| format!("invalid trace json: {}", path.display()))
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

pub(super) fn parse_trace_meta(path: &Path) -> Result<TraceMeta> {
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

pub(super) fn is_manifest(value: &Value) -> bool {
    value.get("trace_schema_version").is_some()
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
