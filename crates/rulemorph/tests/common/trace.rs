#![allow(dead_code)]

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rulemorph::{TraceAttributeValue, TraceEvent, TraceEventKind, TransformTrace};

pub(crate) fn iter_trace_events(trace: &TransformTrace) -> Vec<&TraceEvent> {
    trace
        .records
        .iter()
        .flat_map(|record| record.events.iter())
        .chain(trace.finalize.iter().flatten())
        .collect()
}

pub(crate) fn assert_operator_lifecycle(trace: &TransformTrace, operator: &str) {
    let events = iter_trace_events(trace);
    let op_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart && event.operator.as_deref() == Some(operator)
        })
        .copied()
        .unwrap_or_else(|| panic!("missing op_start for {operator}"));
    let has_end_or_error = events.iter().any(|event| {
        matches!(&event.kind, TraceEventKind::OpEnd | TraceEventKind::OpError)
            && event.operator.as_deref() == Some(operator)
            && event.parent_id == Some(op_start.id)
    });
    assert!(has_end_or_error, "missing op_end/op_error for {operator}");
}

pub(crate) fn assert_parent_ids_point_to_emitted_events(trace: &TransformTrace) {
    let ids = iter_trace_events(trace)
        .into_iter()
        .map(|event| event.id)
        .collect::<BTreeSet<_>>();
    for event in iter_trace_events(trace) {
        if let Some(parent_id) = event.parent_id {
            assert!(
                ids.contains(&parent_id),
                "dangling parent_id {parent_id} on {:?}",
                event.kind
            );
        }
    }
}

pub(crate) fn assert_trace_paths_are_canonical(trace: &TransformTrace) {
    assert_trace_paths_are_canonical_impl(trace, false);
}

pub(crate) fn assert_trace_shape(trace: &TransformTrace) {
    assert_parent_ids_point_to_emitted_events(trace);
    assert_trace_paths_are_canonical_impl(trace, true);
}

fn assert_trace_paths_are_canonical_impl(trace: &TransformTrace, allow_namespace_brackets: bool) {
    for event in iter_trace_events(trace) {
        if let Some(path) = event.input_path.as_deref() {
            assert!(
                path == "@input"
                    || path.starts_with("@input.")
                    || path.starts_with("@input[")
                    || path == "@item"
                    || path.starts_with("@item.")
                    || path.starts_with("@item[")
                    || path == "@acc"
                    || path.starts_with("@acc.")
                    || (allow_namespace_brackets && path.starts_with("@acc["))
                    || path == "@context"
                    || path.starts_with("@context.")
                    || (allow_namespace_brackets && path.starts_with("@context["))
                    || path == "@out"
                    || path.starts_with("@out.")
                    || (allow_namespace_brackets && path.starts_with("@out[")),
                "non-canonical input_path: {path}"
            );
        }
        if let Some(path) = event.output_path.as_deref() {
            assert!(
                path == "$" || path.starts_with("$.") || path.starts_with("$["),
                "non-canonical output_path: {path}"
            );
        }
    }
}

pub(crate) fn assert_trace_does_not_contain_string(trace: &TransformTrace, needle: &str) {
    let value = serde_json::to_value(trace).expect("trace json");
    assert_json_tree_does_not_contain_string(&value, needle);
}

pub(crate) fn assert_no_raw_leak_in_attributes_or_messages(
    trace: &TransformTrace,
    secrets: &[&str],
) {
    for event in iter_trace_events(trace) {
        let metadata = serde_json::json!({
            "attributes": &event.attributes,
            "message": &event.message,
        });
        let text = serde_json::to_string(&metadata).expect("metadata json");
        for secret in secrets {
            assert!(
                !text.contains(secret),
                "secret leaked through attributes/message"
            );
        }
    }
}

pub(crate) fn attr_number(event: &TraceEvent, key: &str) -> Option<u64> {
    match event.attributes.get(key) {
        Some(TraceAttributeValue::Number(number)) => number.as_u64(),
        _ => None,
    }
}

pub(crate) fn attr_bool(event: &TraceEvent, key: &str) -> Option<bool> {
    match event.attributes.get(key) {
        Some(TraceAttributeValue::Bool(flag)) => Some(*flag),
        _ => None,
    }
}

pub(crate) fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("rulemorph-trace-{name}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn assert_json_tree_does_not_contain_string(value: &serde_json::Value, needle: &str) {
    match value {
        serde_json::Value::String(text) => assert!(!text.contains(needle)),
        serde_json::Value::Array(items) => {
            for item in items {
                assert_json_tree_does_not_contain_string(item, needle);
            }
        }
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                assert!(!key.contains(needle));
                assert_json_tree_does_not_contain_string(value, needle);
            }
        }
        _ => {}
    }
}
