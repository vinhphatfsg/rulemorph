use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{Value as JsonValue, json};

use super::TraceWriteOptions;

mod blob;
mod node;
mod traversal;

use blob::{WriteBlobResult, build_payload_preview, write_blob};
use traversal::externalize_trace_payloads_inner;

pub(super) fn externalize_trace_payloads(
    trace: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    if options.max_payload_bytes == 0 {
        return Ok(false);
    }
    let mut seen = HashSet::new();
    externalize_trace_payloads_inner(
        trace,
        trace_dir,
        options,
        &mut seen,
        budget_remaining,
        blob_files,
    )
}

fn maybe_externalize_payload(
    value: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    if value.is_null() || is_externalized_payload(value) {
        return Ok(false);
    }
    let raw = serde_json::to_vec(value)?;
    if raw.len() <= options.max_payload_bytes {
        return Ok(false);
    }
    let preview_limit = options.payload_preview_bytes.min(options.max_payload_bytes);
    let preview = build_payload_preview(&raw, preview_limit);
    match write_blob(trace_dir, &raw, options, seen, budget_remaining)? {
        WriteBlobResult::Written {
            blob_ref,
            blob_path,
        } => {
            if !blob_files.contains(&blob_path) {
                blob_files.push(blob_path);
            }
            *value = json!({
                "preview": preview,
                "size_bytes": raw.len() as u64,
                "blob_ref": blob_ref
            });
            Ok(false)
        }
        WriteBlobResult::BudgetExceeded => Ok(true),
    }
}

fn is_externalized_payload(value: &JsonValue) -> bool {
    value.get("blob_ref").is_some()
        && value.get("size_bytes").is_some()
        && value.get("preview").is_some()
}
