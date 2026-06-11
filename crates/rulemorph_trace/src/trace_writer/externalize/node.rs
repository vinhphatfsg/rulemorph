use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::Value as JsonValue;

use super::maybe_externalize_payload;
use crate::trace_writer::TraceWriteOptions;

pub(super) fn externalize_node_payloads<F>(
    node: &mut JsonValue,
    trace_dir: &Path,
    options: &TraceWriteOptions,
    seen: &mut HashSet<String>,
    budget_remaining: &mut u64,
    blob_files: &mut Vec<PathBuf>,
    externalize_child_trace: &mut F,
) -> Result<bool>
where
    F: FnMut(
        &mut JsonValue,
        &Path,
        &TraceWriteOptions,
        &mut HashSet<String>,
        &mut u64,
        &mut Vec<PathBuf>,
    ) -> Result<bool>,
{
    let Some(obj) = node.as_object_mut() else {
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
    if let Some(args) = obj.get_mut("args")
        && maybe_externalize_payload(args, trace_dir, options, seen, budget_remaining, blob_files)?
    {
        return Ok(true);
    }
    if let Some(pipe_value) = obj.get_mut("pipe_value")
        && maybe_externalize_payload(
            pipe_value,
            trace_dir,
            options,
            seen,
            budget_remaining,
            blob_files,
        )?
    {
        return Ok(true);
    }
    if let Some(pipe_steps) = obj
        .get_mut("pipe_steps")
        .and_then(|value| value.as_array_mut())
    {
        for step in pipe_steps {
            if let Some(step_obj) = step.as_object_mut() {
                if let Some(input) = step_obj.get_mut("input")
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
                if let Some(output) = step_obj.get_mut("output")
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
                externalize_child_trace,
            )? {
                return Ok(true);
            }
        }
    }
    if let Some(child_trace) = obj.get_mut("child_trace")
        && externalize_child_trace(
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
