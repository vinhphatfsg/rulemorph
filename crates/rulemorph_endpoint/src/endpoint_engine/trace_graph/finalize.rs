use std::path::Path;
use std::time::Instant;

use rulemorph::{RuleFile, transform_record_with_base_dir};
use serde_json::{Value as JsonValue, json};

use super::rule_nodes::transform_error_to_trace;
use super::v2_helpers::expr_to_json_value;

pub(super) struct FinalizeTrace {
    pub(super) trace: JsonValue,
    pub(super) pre_finalize_output: Option<JsonValue>,
}

pub(super) fn build_finalize_trace(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Option<FinalizeTrace> {
    let finalize = rule.finalize.as_ref()?;

    let mut base_rule = rule.clone();
    base_rule.finalize = None;
    let base_started = Instant::now();
    let pre_finalize = transform_record_with_base_dir(&base_rule, record, context, base_dir)
        .ok()
        .and_then(|value| value);
    let base_duration_us = base_started.elapsed().as_micros() as u64;
    let pre_finalize_output = pre_finalize.clone();
    let finalize_input = match pre_finalize {
        Some(value) => JsonValue::Array(vec![value]),
        None => JsonValue::Array(Vec::new()),
    };
    let finalize_started = Instant::now();
    let finalize_result = transform_record_with_base_dir(rule, record, context, base_dir);
    let total_duration_us = finalize_started.elapsed().as_micros() as u64;
    let finalize_duration_us = total_duration_us.saturating_sub(base_duration_us);
    let mut finalize_status = "ok";
    let mut finalize_output: Option<JsonValue> = None;
    let mut finalize_error: Option<JsonValue> = None;
    match finalize_result {
        Ok(Some(value)) => {
            finalize_output = Some(value);
        }
        Ok(None) => {
            finalize_output = Some(JsonValue::Null);
        }
        Err(err) => {
            finalize_status = "error";
            finalize_error = Some(transform_error_to_trace(&err));
        }
    }
    let mut children = Vec::new();
    if let Some(filter) = &finalize.filter {
        children.push(json!({
            "id": "op-filter",
            "kind": "op",
            "label": "filter",
            "status": "ok",
            "meta": { "op": "filter" },
            "args": { "expr": expr_to_json_value(filter) }
        }));
    }
    if let Some(sort) = &finalize.sort {
        children.push(json!({
            "id": "op-sort",
            "kind": "op",
            "label": "sort",
            "status": "ok",
            "meta": { "op": "sort" },
            "args": { "by": sort.by, "order": sort.order }
        }));
    }
    if let Some(limit) = finalize.limit {
        children.push(json!({
            "id": "op-limit",
            "kind": "op",
            "label": "limit",
            "status": "ok",
            "meta": { "op": "limit" },
            "args": { "limit": limit }
        }));
    }
    if let Some(offset) = finalize.offset {
        children.push(json!({
            "id": "op-offset",
            "kind": "op",
            "label": "offset",
            "status": "ok",
            "meta": { "op": "offset" },
            "args": { "offset": offset }
        }));
    }
    if let Some(wrap) = &finalize.wrap {
        children.push(json!({
            "id": "op-wrap",
            "kind": "op",
            "label": "wrap",
            "status": "ok",
            "meta": { "op": "wrap" },
            "args": { "wrap": wrap }
        }));
    }

    let mut trace = json!({
        "status": finalize_status,
        "input": finalize_input,
        "output": finalize_output,
        "duration_us": finalize_duration_us,
        "nodes": children,
    });
    if let Some(err) = finalize_error {
        if let Some(obj) = trace.as_object_mut() {
            obj.insert("error".to_string(), err);
        }
    }

    Some(FinalizeTrace {
        trace,
        pre_finalize_output,
    })
}
