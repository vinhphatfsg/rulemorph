use std::path::Path;

use rulemorph::{RuleFile, TransformError, TransformErrorKind};
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use super::branch_trace::apply_branch_trace_meta;
use super::step_outputs::collect_step_outputs;
use super::transform_error_to_trace;
use crate::endpoint_engine::trace_graph::condition::eval_trace_condition;
use crate::endpoint_engine::trace_graph::mapping_ops::build_mapping_ops_with_values;

pub(super) fn build_step_nodes(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: &Path,
) -> Vec<JsonValue> {
    let Some(steps) = rule.steps.as_deref() else {
        return Vec::new();
    };
    let step_outputs = collect_step_outputs(rule, record, context, base_dir);

    let mut nodes = Vec::new();
    let mut prev_output = JsonValue::Object(JsonMap::new());
    let mut halted = false;
    let mut prev_elapsed = 0u64;
    for (index, step) in steps.iter().enumerate() {
        let label = step
            .name
            .clone()
            .unwrap_or_else(|| format!("step-{}", index + 1));
        let kind = if step.branch.is_some() {
            "branch"
        } else if step.record_when.is_some() {
            "record_when"
        } else if step.asserts.is_some() {
            "asserts"
        } else if step.mappings.is_some() {
            "mappings"
        } else {
            "step"
        };

        let step_input = prev_output.clone();
        let mut status = "ok".to_string();
        let mut output_value: Option<JsonValue> = None;
        let mut error: Option<JsonValue> = None;
        let mut child_trace: Option<JsonValue> = None;
        let mut meta = JsonMap::new();

        let step_active = !halted;
        let (step_result, elapsed_total) = match step_outputs.get(index) {
            Some((result, elapsed)) => (result.clone(), *elapsed),
            None => (
                Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    "missing step output",
                )),
                0,
            ),
        };
        let step_duration_us = elapsed_total.saturating_sub(prev_elapsed);
        prev_elapsed = elapsed_total;

        if halted {
            status = "skipped".to_string();
        } else {
            match step_result {
                Ok(Some(out)) => {
                    prev_output = out.clone();
                    output_value = Some(out.clone());
                }
                Ok(None) => {
                    status = "skipped".to_string();
                    output_value = Some(JsonValue::Null);
                    halted = true;
                }
                Err(err) => {
                    status = "error".to_string();
                    error = Some(transform_error_to_trace(&err));
                    halted = true;
                }
            }
        }

        if step_active && status != "error" {
            if let Some(expr) = step.record_when.as_ref() {
                match eval_trace_condition(
                    expr,
                    record,
                    context,
                    &step_input,
                    "record_when",
                    rule.version,
                ) {
                    Ok(flag) => {
                        meta.insert("record_when".to_string(), JsonValue::Bool(flag));
                    }
                    Err(err) => {
                        status = "error".to_string();
                        error = Some(transform_error_to_trace(&err));
                        halted = true;
                    }
                }
            }
        }

        if step_active && status != "error" {
            if let Some(asserts) = step.asserts.as_ref() {
                let mut asserts_ok = true;
                for (assert_index, assert) in asserts.iter().enumerate() {
                    let assert_path = format!("steps[{}].asserts[{}].when", index, assert_index);
                    match eval_trace_condition(
                        &assert.when,
                        record,
                        context,
                        &step_input,
                        &assert_path,
                        rule.version,
                    ) {
                        Ok(true) => {}
                        Ok(false) => {
                            asserts_ok = false;
                            let err = TransformError::new(
                                TransformErrorKind::AssertionFailed,
                                format!(
                                    "assert failed: {}: {}",
                                    assert.error.code, assert.error.message
                                ),
                            )
                            .with_path(format!("steps[{}].asserts[{}]", index, assert_index));
                            status = "error".to_string();
                            error = Some(transform_error_to_trace(&err));
                            halted = true;
                            break;
                        }
                        Err(err) => {
                            asserts_ok = false;
                            status = "error".to_string();
                            error = Some(transform_error_to_trace(&err));
                            halted = true;
                            break;
                        }
                    }
                }
                meta.insert("asserts_ok".to_string(), JsonValue::Bool(asserts_ok));
            }
        }
        if step.asserts.is_some() && !meta.contains_key("asserts_ok") {
            meta.insert("asserts_ok".to_string(), JsonValue::Bool(false));
        }

        if step_active && status != "error" {
            if let Some(branch) = step.branch.as_ref() {
                let branch_taken = match eval_trace_condition(
                    &branch.when,
                    record,
                    context,
                    &step_input,
                    "branch.when",
                    rule.version,
                ) {
                    Ok(true) => "then",
                    Ok(false) => {
                        if branch.r#else.is_some() {
                            "else"
                        } else {
                            "none"
                        }
                    }
                    Err(err) => {
                        status = "error".to_string();
                        error = Some(transform_error_to_trace(&err));
                        halted = true;
                        "none"
                    }
                };
                if branch.return_ && branch_taken != "none" {
                    halted = true;
                }
                child_trace = apply_branch_trace_meta(
                    &branch.then,
                    branch.r#else.as_deref(),
                    branch_taken,
                    base_dir,
                    &step_input,
                    context,
                    &mut meta,
                );
            }
        }

        let children = if status == "ok" {
            if let Some(mappings) = step.mappings.as_deref() {
                let mut mapping_out = step_input.clone();
                build_mapping_ops_with_values(
                    mappings,
                    record,
                    context,
                    &mut mapping_out,
                    rule.version,
                    index,
                )
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        let mut node = json!({
            "id": format!("step-{}", index),
            "kind": kind,
            "label": label,
            "status": status,
            "input": step_input,
            "output": output_value,
            "duration_us": step_duration_us,
        });
        if let Some(err) = error {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("error".to_string(), err);
            }
        }
        if let Some(trace) = child_trace {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("child_trace".to_string(), trace);
            }
        }
        if !meta.is_empty() {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("meta".to_string(), JsonValue::Object(meta));
            }
        }
        if !children.is_empty() {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("children".to_string(), JsonValue::Array(children));
            }
        }
        nodes.push(node);
    }
    nodes
}
