use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};

use super::{TraceCompression, TraceWriteOptions, reserve_budget, write_atomic};

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

fn externalize_node_payloads(
    node: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
) -> Result<bool> {
    let Some(obj) = node.as_object_mut() else {
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
    if let Some(args) = obj.get_mut("args") {
        if maybe_externalize_payload(args, trace_dir, options, seen, budget_remaining, blob_files)?
        {
            return Ok(true);
        }
    }
    if let Some(pipe_value) = obj.get_mut("pipe_value") {
        if maybe_externalize_payload(
            pipe_value,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )? {
            return Ok(true);
        }
    }
    if let Some(pipe_steps) = obj
        .get_mut("pipe_steps")
        .and_then(|value| value.as_array_mut())
    {
        for step in pipe_steps {
            if let Some(step_obj) = step.as_object_mut() {
                if let Some(input) = step_obj.get_mut("input") {
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
                if let Some(output) = step_obj.get_mut("output") {
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
            }
        }
    }
    if let Some(children) = obj
        .get_mut("children")
        .and_then(|value| value.as_array_mut())
    {
        for child in children {
            if externalize_node_payloads(
                child,
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

fn build_payload_preview(raw: &[u8], limit: usize) -> String {
    if limit == 0 || raw.is_empty() {
        return String::new();
    }
    if raw.len() <= limit {
        return String::from_utf8_lossy(raw).into_owned();
    }
    let mut preview = String::from_utf8_lossy(&raw[..limit]).into_owned();
    preview.push_str("...");
    preview
}

enum WriteBlobResult {
    Written {
        blob_ref: String,
        blob_path: PathBuf,
    },
    BudgetExceeded,
}

fn write_blob(
    trace_dir: &Path,
    raw: &[u8],
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
) -> Result<WriteBlobResult> {
    let hash = Sha256::digest(raw);
    let hash_hex = hex_encode(&hash);
    let extension = match options.compression {
        TraceCompression::Zstd => ".zst",
        TraceCompression::None => "",
    };
    let filename = format!("sha256-{hash_hex}.json{extension}");
    let rel_path = PathBuf::from("blobs").join(filename);
    let rel_string = rel_path.to_string_lossy().to_string();
    let full_path = trace_dir.join(&rel_path);
    if !seen.insert(rel_string.clone()) {
        return Ok(WriteBlobResult::Written {
            blob_ref: rel_string,
            blob_path: full_path,
        });
    }
    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file_exists = full_path.exists();
    let mut needs_write = !file_exists;
    let mut payload: Option<Vec<u8>> = None;
    let bytes = if file_exists {
        match fs::metadata(&full_path) {
            Ok(metadata) => metadata.len(),
            Err(_) => {
                let computed = match options.compression {
                    TraceCompression::Zstd => zstd::stream::encode_all(raw, 3)?,
                    TraceCompression::None => raw.to_vec(),
                };
                let bytes = computed.len() as u64;
                payload = Some(computed);
                needs_write = true;
                bytes
            }
        }
    } else {
        let computed = match options.compression {
            TraceCompression::Zstd => zstd::stream::encode_all(raw, 3)?,
            TraceCompression::None => raw.to_vec(),
        };
        let bytes = computed.len() as u64;
        payload = Some(computed);
        bytes
    };
    if !reserve_budget(budget_remaining, bytes) {
        return Ok(WriteBlobResult::BudgetExceeded);
    }
    if needs_write {
        if let Some(payload) = payload {
            write_atomic(&full_path, payload.as_slice())?;
        }
    }
    Ok(WriteBlobResult::Written {
        blob_ref: rel_string,
        blob_path: full_path,
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{:02x}", byte));
    }
    output
}
