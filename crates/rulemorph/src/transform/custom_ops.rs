use std::collections::BTreeMap;

use serde_json::{Map, Value as JsonValue};

use super::{
    EvalLimits, MappingEvalInput, MappingTraceInput, Namespace, eval_mapping_traced,
    eval_mapping_traced_with_source_redaction_hint, eval_mapping_with_v2_context,
    eval_v2_expr_traced, eval_v2_pipe_traced, eval_when_expr_traced_with_v2_context,
    eval_when_expr_with_v2_context, expr_to_json_for_v2_pipe, parse_source, set_path,
};
use crate::custom_ops::{self, ContractMode};
use crate::error::{TransformError, TransformErrorKind};
use crate::model::{CustomOpDef, Expr, Mapping, RuleFile, RuleType, RuleTypeField, RuleTypeKind};
use crate::path::{PathToken, parse_path};
use crate::trace::{
    TraceCollector, TraceEventKind, TracePhase, canonical_acc_path, canonical_context_path,
    canonical_input_path, canonical_item_path, canonical_out_path, canonical_output_path,
};
use crate::v2_eval::{EvalValue as V2EvalValue, V2EvalContext, eval_v2_expr, eval_v2_pipe};
use crate::v2_model::{
    V2CallArg, V2Condition, V2CustomCallStep, V2Expr, V2ObjectFieldValue, V2OpStep, V2Pipe, V2Ref,
    V2Start, V2Step,
};
use crate::v2_parser::{
    custom_call_step_candidate, parse_custom_call_step, parse_v2_pipe_from_value,
};

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
mod body;
mod input;
mod redaction;
mod returns;

use body::*;
use input::*;
use redaction::*;
use returns::*;

pub(crate) fn eval_custom_op_step<'a>(
    op_step: &V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<Option<V2EvalValue>, TransformError> {
    let Some(rule) = ctx.rule() else {
        return Ok(None);
    };
    if !rule.defs.contains_key(&op_step.op) {
        return Ok(None);
    }
    if custom_ops::is_reserved_or_builtin_custom_op_name(&op_step.op) {
        return Err(shadowed_custom_op_error(&op_step.op, path));
    }
    if !op_step.args.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "custom op arguments must use with call options",
        )
        .with_path(path));
    }
    eval_custom_op(
        rule,
        &op_step.op,
        pipe_value,
        None,
        record,
        context,
        out,
        path,
        ctx,
    )
    .map(Some)
}

pub(crate) fn parse_known_custom_call_literal_start(
    start: &V2Start,
    ctx: &V2EvalContext<'_>,
    path: &str,
) -> Result<Option<V2CustomCallStep>, TransformError> {
    let V2Start::Literal(value) = start else {
        return Ok(None);
    };
    let Some((op_name, args_val)) = custom_call_step_candidate(value) else {
        return Ok(None);
    };
    if !ctx
        .rule()
        .is_some_and(|rule| rule.defs.contains_key(op_name))
    {
        return Ok(None);
    }
    match parse_custom_call_step(op_name, args_val) {
        Ok(Some(call)) => Ok(Some(call)),
        Ok(None) => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "invalid custom op call: custom op call must use with call options",
        )
        .with_path(path)),
        Err(err) => Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid custom op call: {}", err),
        )
        .with_path(path)),
    }
}

pub(crate) fn eval_custom_call_step<'a>(
    call: &V2CustomCallStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<V2EvalValue, TransformError> {
    let Some(rule) = ctx.rule() else {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("unknown custom op: {}", call.op),
        )
        .with_path(path));
    };
    eval_custom_op(
        rule,
        &call.op,
        pipe_value,
        call.with.as_deref(),
        record,
        context,
        out,
        path,
        ctx,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(crate) fn eval_custom_op_step_traced<'a>(
    op_step: &V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<Option<V2EvalValue>, TransformError> {
    let Some(rule) = ctx.rule() else {
        return Ok(None);
    };
    if !rule.defs.contains_key(&op_step.op) {
        return Ok(None);
    }
    if custom_ops::is_reserved_or_builtin_custom_op_name(&op_step.op) {
        return Err(shadowed_custom_op_error(&op_step.op, path));
    }
    if !op_step.args.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "custom op arguments must use with call options",
        )
        .with_path(path));
    }
    eval_custom_op_traced(
        rule,
        &op_step.op,
        pipe_value,
        None,
        record,
        context,
        out,
        path,
        ctx,
        collector,
    )
    .map(Some)
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(crate) fn eval_custom_call_step_traced<'a>(
    call: &V2CustomCallStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let Some(rule) = ctx.rule() else {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("unknown custom op: {}", call.op),
        )
        .with_path(path));
    };
    eval_custom_op_traced(
        rule,
        &call.op,
        pipe_value,
        call.with.as_deref(),
        record,
        context,
        out,
        path,
        ctx,
        collector,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
fn eval_custom_op<'a>(
    rule: &'a RuleFile,
    name: &str,
    pipe_value: V2EvalValue,
    with: Option<&[(String, V2CallArg)]>,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<V2EvalValue, TransformError> {
    eval_custom_op_inner(
        rule, name, pipe_value, with, record, context, out, path, ctx, None,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
fn eval_custom_op_traced<'a>(
    rule: &'a RuleFile,
    name: &str,
    pipe_value: V2EvalValue,
    with: Option<&[(String, V2CallArg)]>,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    eval_custom_op_inner(
        rule,
        name,
        pipe_value,
        with,
        record,
        context,
        out,
        path,
        ctx,
        Some(collector),
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
fn eval_custom_op_inner<'a>(
    rule: &'a RuleFile,
    name: &str,
    pipe_value: V2EvalValue,
    with: Option<&[(String, V2CallArg)]>,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    mut collector: Option<&mut TraceCollector>,
) -> Result<V2EvalValue, TransformError> {
    if custom_ops::is_reserved_or_builtin_custom_op_name(name) {
        return Err(shadowed_custom_op_error(name, path));
    }
    let def = rule.defs.get(name).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            format!("unknown custom op: {}", name),
        )
        .with_path(path)
    })?;
    let limits = ctx.limits();
    if ctx.custom_op_depth() >= limits.max_custom_op_call_depth {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "custom op call depth exceeds configured limit",
        )
        .with_path(path));
    }
    if ctx.increment_custom_op_calls() > limits.max_custom_op_calls_per_record {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "custom op calls per record exceed configured limit",
        )
        .with_path(path));
    }

    let (input, mode, input_redaction_hints) = match with {
        Some(with) => {
            let with_ctx = ctx
                .clone()
                .with_custom_op_depth(ctx.custom_op_depth().saturating_add(1));
            if let Some(collector) = collector.as_deref_mut() {
                let custom_body_input_scope = ctx.custom_op_depth() > 0;
                let traced_input = eval_with_object_traced(TracedWithObjectInput {
                    with,
                    input_type: &def.input,
                    record,
                    context,
                    out,
                    path,
                    custom_body_input_scope,
                    ctx: &with_ctx,
                    collector,
                })?;
                (
                    traced_input.value,
                    ContractMode::AdapterExact,
                    traced_input.redaction_hints,
                )
            } else {
                (
                    eval_with_object(with, &def.input, record, context, out, path, &with_ctx)?,
                    ContractMode::AdapterExact,
                    CustomInputRedactionHints::default(),
                )
            }
        }
        None => match pipe_value {
            V2EvalValue::Value(value) => (
                value,
                ContractMode::InputWidth,
                CustomInputRedactionHints::default(),
            ),
            V2EvalValue::Missing => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "custom op input contract mismatch: input is missing",
                )
                .with_path(path));
            }
        },
    };

    custom_ops::check_contract(&input, &def.input, mode, path).map_err(|err| {
        TransformError::new(
            err.kind,
            format!("custom op input contract mismatch: {}", err.message),
        )
        .with_path(path)
    })?;

    let output = if let Some(collector) = collector {
        eval_custom_op_body_traced(
            rule,
            name,
            def,
            &input,
            &input_redaction_hints,
            path,
            limits,
            ctx,
            collector,
        )?
    } else {
        eval_custom_op_body(rule, name, def, &input, path, limits, ctx)?
    };
    let returns = def
        .returns
        .as_ref()
        .cloned()
        .unwrap_or_else(|| synthesize_mappings_return(def.mappings.as_deref().unwrap_or(&[])));
    custom_ops::check_contract(&output, &returns, ContractMode::OutputExact, path).map_err(
        |err| {
            TransformError::new(
                err.kind,
                format!("custom op output contract mismatch: {}", err.message),
            )
            .with_path(path)
        },
    )?;
    Ok(V2EvalValue::Value(output))
}
