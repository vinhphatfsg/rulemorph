use std::path::Path;

use rulemorph::v2_eval::V2EvalContext;
use rulemorph::{RuleFile, TransformError, TransformErrorKind};
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::super::branch_trace::apply_branch_trace_meta;
use super::super::transform_error_to_trace;
use crate::endpoint_engine::trace_graph::condition::eval_trace_condition;

pub(super) fn apply_record_when_meta(
    rule: &RuleFile,
    step_index: usize,
    record: &JsonValue,
    context: Option<&JsonValue>,
    step_input: &JsonValue,
    step_active: bool,
    status: &mut String,
    error: &mut Option<JsonValue>,
    halted: &mut bool,
    meta: &mut JsonMap<String, JsonValue>,
    trace_ctx: &V2EvalContext<'_>,
) {
    if !step_active || status == "error" {
        return;
    }
    let Some(expr) = rule
        .steps
        .as_deref()
        .and_then(|steps| steps.get(step_index))
        .and_then(|step| step.record_when.as_ref())
    else {
        return;
    };

    match eval_trace_condition(
        rule,
        expr,
        record,
        context,
        step_input,
        "record_when",
        trace_ctx,
    ) {
        Ok(flag) => {
            meta.insert("record_when".to_string(), JsonValue::Bool(flag));
        }
        Err(err) => {
            *status = "error".to_string();
            *error = Some(transform_error_to_trace(&err));
            *halted = true;
        }
    }
}

pub(super) fn apply_asserts_meta(
    rule: &RuleFile,
    step_index: usize,
    record: &JsonValue,
    context: Option<&JsonValue>,
    step_input: &JsonValue,
    step_active: bool,
    status: &mut String,
    error: &mut Option<JsonValue>,
    halted: &mut bool,
    meta: &mut JsonMap<String, JsonValue>,
    trace_ctx: &V2EvalContext<'_>,
) {
    let Some(asserts) = rule
        .steps
        .as_deref()
        .and_then(|steps| steps.get(step_index))
        .and_then(|step| step.asserts.as_ref())
    else {
        return;
    };

    if !step_active || status == "error" {
        meta.entry("asserts_ok".to_string())
            .or_insert(JsonValue::Bool(false));
        return;
    }

    let mut asserts_ok = true;
    for (assert_index, assert) in asserts.iter().enumerate() {
        let assert_path = format!("steps[{}].asserts[{}].when", step_index, assert_index);
        match eval_trace_condition(
            rule,
            &assert.when,
            record,
            context,
            step_input,
            &assert_path,
            trace_ctx,
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
                .with_path(format!("steps[{}].asserts[{}]", step_index, assert_index));
                *status = "error".to_string();
                *error = Some(transform_error_to_trace(&err));
                *halted = true;
                break;
            }
            Err(err) => {
                asserts_ok = false;
                *status = "error".to_string();
                *error = Some(transform_error_to_trace(&err));
                *halted = true;
                break;
            }
        }
    }
    meta.insert("asserts_ok".to_string(), JsonValue::Bool(asserts_ok));
}

pub(super) fn apply_branch_meta(
    rule: &RuleFile,
    step_index: usize,
    record: &JsonValue,
    context: Option<&JsonValue>,
    base_dir: &Path,
    step_input: &JsonValue,
    step_active: bool,
    status: &mut String,
    error: &mut Option<JsonValue>,
    halted: &mut bool,
    meta: &mut JsonMap<String, JsonValue>,
    trace_ctx: &V2EvalContext<'_>,
) -> Option<JsonValue> {
    if !step_active || status == "error" {
        return None;
    }
    let Some(branch) = rule
        .steps
        .as_deref()
        .and_then(|steps| steps.get(step_index))
        .and_then(|step| step.branch.as_ref())
    else {
        return None;
    };

    let branch_taken = match eval_trace_condition(
        rule,
        &branch.when,
        record,
        context,
        step_input,
        "branch.when",
        trace_ctx,
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
            *status = "error".to_string();
            *error = Some(transform_error_to_trace(&err));
            *halted = true;
            "none"
        }
    };
    if branch.return_ && branch_taken != "none" {
        *halted = true;
    }
    apply_branch_trace_meta(
        &branch.then,
        branch.r#else.as_deref(),
        branch_taken,
        base_dir,
        step_input,
        context,
        meta,
    )
}
