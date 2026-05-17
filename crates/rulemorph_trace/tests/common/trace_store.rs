use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Result, bail};
use serde_json::Value;

pub fn create_trace_dir(data_dir: &Path, relative_trace_dir: impl AsRef<Path>) -> Result<PathBuf> {
    let relative_trace_dir = relative_trace_dir.as_ref();
    if relative_trace_dir.is_absolute()
        || relative_trace_dir
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        bail!("trace fixture path must be relative to the data dir");
    }

    let trace_dir = data_dir.join(relative_trace_dir);
    fs::create_dir_all(&trace_dir)?;
    Ok(trace_dir)
}

pub fn write_trace_json(trace_dir: &Path, payload: &Value) -> Result<()> {
    fs::write(trace_dir.join("trace.json"), serde_json::to_vec(payload)?)?;
    Ok(())
}

pub fn detail_object(trace: &Value) -> &serde_json::Map<String, Value> {
    trace
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object")
}

pub fn assert_detail_status(trace: &Value, expected: &str) {
    assert_eq!(
        detail_object(trace)
            .get("status")
            .and_then(|value| value.as_str()),
        Some(expected)
    );
}

pub fn assert_detail_reason(trace: &Value, expected: &str) {
    let reasons = detail_object(trace)
        .get("reason")
        .and_then(|value| value.as_array());
    assert!(
        reasons.is_some_and(|reasons| reasons.iter().any(|value| value.as_str() == Some(expected))),
        "expected detail reason {expected:?}, got {reasons:?}"
    );
}

pub fn assert_detail_array_empty(trace: &Value, key: &str) {
    let is_empty = detail_object(trace)
        .get(key)
        .and_then(|value| value.as_array())
        .map_or(true, |items| items.is_empty());
    assert!(is_empty, "detail.{key} should be empty");
}

pub fn assert_top_level_array_empty(trace: &Value, key: &str) {
    let is_empty = trace
        .get(key)
        .and_then(|value| value.as_array())
        .map_or(true, |items| items.is_empty());
    assert!(is_empty, "{key} should be empty");
}

pub fn assert_finalize_absent(trace: &Value) {
    assert!(detail_object(trace).get("finalize").is_none());
    assert!(trace.get("finalize").is_none());
}
