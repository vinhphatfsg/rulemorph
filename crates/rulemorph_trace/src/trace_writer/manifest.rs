use std::fs;
use std::path::PathBuf;

use serde_json::Value as JsonValue;
use tracing::warn;

use crate::trace_schema::{RuleMeta, TraceSummary};

pub(super) fn parse_rule_meta(value: &JsonValue) -> RuleMeta {
    RuleMeta {
        name: value
            .get("name")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string()),
        path: value
            .get("path")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string()),
        r#type: value
            .get("type")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string()),
        version: value
            .get("version")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8),
    }
}

pub(super) fn parse_summary(value: &JsonValue) -> TraceSummary {
    TraceSummary {
        record_total: value.get("record_total").and_then(|v| v.as_u64()),
        record_success: value.get("record_success").and_then(|v| v.as_u64()),
        record_failed: value.get("record_failed").and_then(|v| v.as_u64()),
        duration_ms: value.get("duration_ms").and_then(|v| v.as_u64()),
        duration_us: value.get("duration_us").and_then(|v| v.as_u64()),
    }
}

pub(super) fn reserve_budget(remaining: &mut u64, bytes: u64) -> bool {
    if bytes == 0 {
        return true;
    }
    if *remaining < bytes {
        return false;
    }
    *remaining -= bytes;
    true
}

pub(super) fn count_inline_nodes(records: &[JsonValue], max_nodes: usize) -> usize {
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

pub(super) fn blob_total_bytes(blob_files: &[PathBuf]) -> u64 {
    blob_files
        .iter()
        .map(|path| match fs::metadata(path) {
            Ok(meta) => meta.len(),
            Err(err) => {
                warn!("failed to read blob metadata {}: {}", path.display(), err);
                0
            }
        })
        .sum()
}
