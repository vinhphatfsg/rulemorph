use std::path::Path;

use rulemorph::v2_eval::V2EvalContext;
use rulemorph::{RuleFile, TransformError, TransformErrorKind};
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::super::branch_trace::apply_branch_trace_meta;
use super::super::transform_error_to_trace;
use crate::endpoint_engine::trace_graph::condition::eval_trace_condition;

pub(super) struct StepConditionContext<'a, 'ctx> {
    pub(super) rule: &'a RuleFile,
    pub(super) step_index: usize,
    pub(super) record: &'a JsonValue,
    pub(super) context: Option<&'a JsonValue>,
    pub(super) base_dir: &'a Path,
    pub(super) step_input: &'a JsonValue,
    pub(super) step_active: bool,
    pub(super) trace_ctx: &'a V2EvalContext<'ctx>,
}

pub(super) struct StepMetaState<'a> {
    pub(super) status: &'a mut String,
    pub(super) error: &'a mut Option<JsonValue>,
    pub(super) halted: &'a mut bool,
    pub(super) meta: &'a mut JsonMap<String, JsonValue>,
}

pub(super) fn apply_record_when_meta(
    context: &StepConditionContext<'_, '_>,
    state: &mut StepMetaState<'_>,
) {
    if !context.step_active || state.status == "error" {
        return;
    }
    let Some(expr) = context
        .rule
        .steps
        .as_deref()
        .and_then(|steps| steps.get(context.step_index))
        .and_then(|step| step.record_when.as_ref())
    else {
        return;
    };

    match eval_trace_condition(
        context.rule,
        expr,
        context.record,
        context.context,
        context.step_input,
        "record_when",
        context.trace_ctx,
    ) {
        Ok(flag) => {
            state
                .meta
                .insert("record_when".to_string(), JsonValue::Bool(flag));
        }
        Err(err) => {
            *state.status = "error".to_string();
            *state.error = Some(transform_error_to_trace(&err));
            *state.halted = true;
        }
    }
}

pub(super) fn apply_asserts_meta(
    context: &StepConditionContext<'_, '_>,
    state: &mut StepMetaState<'_>,
) {
    let Some(asserts) = context
        .rule
        .steps
        .as_deref()
        .and_then(|steps| steps.get(context.step_index))
        .and_then(|step| step.asserts.as_ref())
    else {
        return;
    };

    if !context.step_active || state.status == "error" {
        state
            .meta
            .entry("asserts_ok".to_string())
            .or_insert(JsonValue::Bool(false));
        return;
    }

    let mut asserts_ok = true;
    for (assert_index, assert) in asserts.iter().enumerate() {
        let assert_path = format!(
            "steps[{}].asserts[{}].when",
            context.step_index, assert_index
        );
        match eval_trace_condition(
            context.rule,
            &assert.when,
            context.record,
            context.context,
            context.step_input,
            &assert_path,
            context.trace_ctx,
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
                .with_path(format!(
                    "steps[{}].asserts[{}]",
                    context.step_index, assert_index
                ));
                *state.status = "error".to_string();
                *state.error = Some(transform_error_to_trace(&err));
                *state.halted = true;
                break;
            }
            Err(err) => {
                asserts_ok = false;
                *state.status = "error".to_string();
                *state.error = Some(transform_error_to_trace(&err));
                *state.halted = true;
                break;
            }
        }
    }
    state
        .meta
        .insert("asserts_ok".to_string(), JsonValue::Bool(asserts_ok));
}

pub(super) fn apply_branch_meta(
    context: &StepConditionContext<'_, '_>,
    state: &mut StepMetaState<'_>,
) -> Option<JsonValue> {
    if !context.step_active || state.status == "error" {
        return None;
    }
    let branch = context
        .rule
        .steps
        .as_deref()
        .and_then(|steps| steps.get(context.step_index))
        .and_then(|step| step.branch.as_ref())?;

    let branch_taken = match eval_trace_condition(
        context.rule,
        &branch.when,
        context.record,
        context.context,
        context.step_input,
        "branch.when",
        context.trace_ctx,
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
            *state.status = "error".to_string();
            *state.error = Some(transform_error_to_trace(&err));
            *state.halted = true;
            "none"
        }
    };
    if branch.return_ && branch_taken != "none" {
        *state.halted = true;
    }
    apply_branch_trace_meta(
        &branch.then,
        branch.r#else.as_deref(),
        branch_taken,
        context.base_dir,
        context.step_input,
        context.context,
        state.meta,
    )
}
