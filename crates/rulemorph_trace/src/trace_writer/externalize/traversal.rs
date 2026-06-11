use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::Value as JsonValue;

use super::{maybe_externalize_payload, node::externalize_node_payloads};
use crate::trace_writer::TraceWriteOptions;

pub(super) fn externalize_trace_payloads_inner(
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
    if let Some(finalize) = obj.get_mut("finalize")
        && externalize_finalize_payloads(
            finalize,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )?
    {
        return Ok(true);
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
    if let Some(input) = obj.get_mut("input")
        && maybe_externalize_payload(
            input,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )?
    {
        return Ok(true);
    }
    if let Some(output) = obj.get_mut("output")
        && maybe_externalize_payload(
            output,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )?
    {
        return Ok(true);
    }
    if let Some(nodes_value) = obj.get_mut("nodes") {
        match nodes_value {
            JsonValue::Array(nodes) => {
                for node in nodes {
                    let mut externalize_child_trace = externalize_trace_payloads_inner;
                    if externalize_node_payloads(
                        node,
                        trace_dir,
                        options,
                        seen,
                        budget_remaining,
                        blob_files,
                        &mut externalize_child_trace,
                    )? {
                        return Ok(true);
                    }
                }
            }
            JsonValue::Object(_) => {
                let mut externalize_child_trace = externalize_trace_payloads_inner;
                if externalize_node_payloads(
                    nodes_value,
                    trace_dir,
                    options,
                    seen,
                    budget_remaining,
                    blob_files,
                    &mut externalize_child_trace,
                )? {
                    return Ok(true);
                }
            }
            _ => {}
        }
    }
    if let Some(child_trace) = obj.get_mut("child_trace")
        && externalize_trace_payloads_inner(
            child_trace,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )?
    {
        return Ok(true);
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
    if let Some(input) = obj.get_mut("input")
        && maybe_externalize_payload(
            input,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )?
    {
        return Ok(true);
    }
    if let Some(output) = obj.get_mut("output")
        && maybe_externalize_payload(
            output,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )?
    {
        return Ok(true);
    }
    if let Some(nodes) = obj.get_mut("nodes").and_then(|value| value.as_array_mut()) {
        for node in nodes {
            let mut externalize_child_trace = externalize_trace_payloads_inner;
            if externalize_node_payloads(
                node,
                trace_dir,
                options,
                seen,
                budget_remaining,
                blob_files,
                &mut externalize_child_trace,
            )? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
