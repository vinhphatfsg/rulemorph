use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{Value as JsonValue, json};

use super::TraceWriteOptions;

mod blob;
mod node;

use blob::{WriteBlobResult, build_payload_preview, write_blob};
use node::externalize_node_payloads;

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

fn externalize_trace_payloads_inner(
    trace: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    let Some(obj) = trace.as_object_mut() else {
        return Ok(false);
    };
    if let Some(records) = obj
        .get_mut("records")
        .and_then(|value| value.as_array_mut())
    {
        for record in records {
            if externalize_record_payloads(
                record,
                trace_dir,
                options,
                seen,
                budget_remaining,
                blob_files,
            )? {
                return Ok(true);
            }
        }
    }
    if let Some(finalize) = obj.get_mut("finalize") {
        if externalize_finalize_payloads(
            finalize,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn externalize_record_payloads(
    record: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    let Some(obj) = record.as_object_mut() else {
        return Ok(false);
    };
    if let Some(input) = obj.get_mut("input") {
        if maybe_externalize_payload(
            input,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(output) = obj.get_mut("output") {
        if maybe_externalize_payload(
            output,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(nodes_value) = obj.get_mut("nodes") {
        match nodes_value {
            JsonValue::Array(nodes) => {
                for node in nodes {
                    if externalize_node_payloads(
                        node,
                        trace_dir,
                        options,
                        seen,
                        budget_remaining,
                        blob_files,
                    )? {
                        return Ok(true);
                    }
                }
            }
            JsonValue::Object(_) => {
                if externalize_node_payloads(
                    nodes_value,
                    trace_dir,
                    options,
                    seen,
                    budget_remaining,
                    blob_files,
                )? {
                    return Ok(true);
                }
            }
            _ => {}
        }
    }
    if let Some(child_trace) = obj.get_mut("child_trace") {
        if externalize_trace_payloads_inner(
            child_trace,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn externalize_finalize_payloads(
    finalize: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    let Some(obj) = finalize.as_object_mut() else {
        return Ok(false);
    };
    if let Some(input) = obj.get_mut("input") {
        if maybe_externalize_payload(
            input,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(output) = obj.get_mut("output") {
        if maybe_externalize_payload(
            output,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(nodes) = obj.get_mut("nodes").and_then(|value| value.as_array_mut()) {
        for node in nodes {
            if externalize_node_payloads(
                node,
                trace_dir,
                options,
                seen,
                budget_remaining,
                blob_files,
            )? {
                return Ok(true);
            }
        }
    }
    Ok(false)
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
