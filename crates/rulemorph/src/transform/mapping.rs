use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::trace::{
    TraceCollector, TraceEventKind, TracePhase, canonical_context_path, canonical_input_path,
    canonical_out_path,
};
use crate::v2_eval::{EvalValue as V2EvalValue, V2EvalContext, eval_v2_pipe};
use crate::v2_parser::parse_v2_pipe_from_value;

use super::{
    EvalLimits, EvalLocals, EvalValue, Namespace, cast_value, eval_expr, eval_expr_traced,
    eval_v2_pipe_traced, expr_to_json_for_v2_pipe, parse_source, resolve_source,
};

pub(super) fn eval_mapping_with_v2_context(
    rule: &crate::model::RuleFile,
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    version: u8,
    limits: EvalLimits,
    base_v2_ctx: &V2EvalContext<'_>,
) -> Result<Option<JsonValue>, TransformError> {
    eval_mapping_inner(
        rule,
        mapping,
        record,
        context,
        out,
        mapping_path,
        version,
        limits,
        Some(base_v2_ctx),
    )
}

#[allow(clippy::too_many_arguments)]
fn eval_mapping_inner(
    rule: &crate::model::RuleFile,
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    version: u8,
    limits: EvalLimits,
    base_v2_ctx: Option<&V2EvalContext<'_>>,
) -> Result<Option<JsonValue>, TransformError> {
    let value = if let Some(source) = &mapping.source {
        resolve_source(source, record, context, out, mapping_path)?
    } else if let Some(literal) = &mapping.value {
        EvalValue::Value(literal.clone())
    } else if let Some(expr) = &mapping.expr {
        // Check if this is a v2 expression (version 2)
        if version >= 2 {
            let expr_path = format!("{}.expr", mapping_path);
            // Try to interpret as v2 pipe
            let v2_json = expr_to_json_for_v2_pipe(expr);
            if let Some(json_val) = v2_json {
                let v2_pipe = parse_v2_pipe_from_value(&json_val).map_err(|e| {
                    TransformError::new(TransformErrorKind::ExprError, e.to_string())
                        .with_path(&expr_path)
                })?;
                let v2_ctx = base_v2_ctx
                    .cloned()
                    .unwrap_or_else(V2EvalContext::new)
                    .with_limits(limits)
                    .with_rule(rule);
                let v2_result = eval_v2_pipe(&v2_pipe, record, context, out, &expr_path, &v2_ctx)?;
                // Convert v2 EvalValue to v1 EvalValue
                match v2_result {
                    V2EvalValue::Missing => EvalValue::Missing,
                    V2EvalValue::Value(v) => EvalValue::Value(v),
                }
            } else {
                // v2 but not a v2 pipe - use v1 eval
                let eval_locals = root_eval_locals(limits);
                eval_expr(expr, record, context, out, &expr_path, Some(&eval_locals))?
            }
        } else {
            // v1 rule - use v1 eval
            let eval_locals = root_eval_locals(limits);
            eval_expr(
                expr,
                record,
                context,
                out,
                &format!("{}.expr", mapping_path),
                Some(&eval_locals),
            )?
        }
    } else {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "mapping must define source, value, or expr",
        )
        .with_path(mapping_path));
    };

    let mut value = match value {
        EvalValue::Missing => {
            if let Some(default) = &mapping.default {
                default.clone()
            } else if mapping.required {
                return Err(TransformError::new(
                    TransformErrorKind::MissingRequired,
                    "required value is missing",
                )
                .with_path(mapping_path));
            } else {
                return Ok(None);
            }
        }
        EvalValue::Value(value) => value,
    };

    if value.is_null() {
        if mapping.required {
            return Err(TransformError::new(
                TransformErrorKind::MissingRequired,
                "required value is null",
            )
            .with_path(mapping_path));
        }
        return Ok(Some(value));
    }

    if let Some(type_name) = &mapping.value_type {
        value = cast_value(&value, type_name, &format!("{}.type", mapping_path))?;
    }

    Ok(Some(value))
}

pub(super) fn eval_mapping_traced(
    rule: &crate::model::RuleFile,
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    version: u8,
    limits: EvalLimits,
    base_v2_ctx: &V2EvalContext<'_>,
    collector: &mut TraceCollector,
) -> Result<Option<JsonValue>, TransformError> {
    eval_mapping_traced_inner(
        rule,
        mapping,
        record,
        context,
        out,
        mapping_path,
        version,
        limits,
        base_v2_ctx,
        collector,
        SourceRedactionHint::DefaultSource,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_mapping_traced_with_source_redaction_hint(
    rule: &crate::model::RuleFile,
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    version: u8,
    limits: EvalLimits,
    base_v2_ctx: &V2EvalContext<'_>,
    collector: &mut TraceCollector,
    source_redaction_hint: Option<&str>,
) -> Result<Option<JsonValue>, TransformError> {
    eval_mapping_traced_inner(
        rule,
        mapping,
        record,
        context,
        out,
        mapping_path,
        version,
        limits,
        base_v2_ctx,
        collector,
        SourceRedactionHint::Override(source_redaction_hint),
    )
}

enum SourceRedactionHint<'a> {
    DefaultSource,
    Override(Option<&'a str>),
}

#[allow(clippy::too_many_arguments)]
fn eval_mapping_traced_inner(
    rule: &crate::model::RuleFile,
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    version: u8,
    limits: EvalLimits,
    base_v2_ctx: &V2EvalContext<'_>,
    collector: &mut TraceCollector,
    source_redaction_hint: SourceRedactionHint<'_>,
) -> Result<Option<JsonValue>, TransformError> {
    let value = if let Some(source) = &mapping.source {
        let value = resolve_source(source, record, context, out, mapping_path)?;
        let path_hint = match source_redaction_hint {
            SourceRedactionHint::DefaultSource => Some(source.as_str()),
            SourceRedactionHint::Override(path_hint) => path_hint,
        };
        collector
            .emit(TraceEventKind::SourceRead, TracePhase::Instant)
            .rule_path(format!("{}.source", mapping_path))
            .input_path(canonical_source_path(source))
            .finish_with_eval_output(collector, &value, path_hint);
        value
    } else if let Some(literal) = &mapping.value {
        collector
            .emit(TraceEventKind::LiteralEval, TracePhase::Instant)
            .rule_path(format!("{}.value", mapping_path))
            .finish_with_output(collector, literal, None);
        EvalValue::Value(literal.clone())
    } else if let Some(expr) = &mapping.expr {
        if version >= 2 {
            let expr_path = format!("{}.expr", mapping_path);
            let v2_json = expr_to_json_for_v2_pipe(expr);
            if let Some(json_val) = v2_json {
                let v2_pipe = parse_v2_pipe_from_value(&json_val).map_err(|e| {
                    TransformError::new(TransformErrorKind::ExprError, e.to_string())
                        .with_path(&expr_path)
                })?;
                let v2_ctx = base_v2_ctx.clone().with_limits(limits).with_rule(rule);
                let v2_result = eval_v2_pipe_traced(
                    &v2_pipe, record, context, out, &expr_path, &v2_ctx, collector,
                )?;
                match v2_result {
                    V2EvalValue::Missing => EvalValue::Missing,
                    V2EvalValue::Value(v) => EvalValue::Value(v),
                }
            } else {
                let eval_locals = root_eval_locals(limits);
                eval_expr_traced(
                    expr,
                    record,
                    context,
                    out,
                    &expr_path,
                    Some(&eval_locals),
                    collector,
                )?
            }
        } else {
            let eval_locals = root_eval_locals(limits);
            eval_expr_traced(
                expr,
                record,
                context,
                out,
                &format!("{}.expr", mapping_path),
                Some(&eval_locals),
                collector,
            )?
        }
    } else {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "mapping must define source, value, or expr",
        )
        .with_path(mapping_path));
    };

    let mut value = match value {
        EvalValue::Missing => {
            if let Some(default) = &mapping.default {
                collector
                    .emit(TraceEventKind::DefaultApplied, TracePhase::Instant)
                    .rule_path(format!("{}.default", mapping_path))
                    .finish_with_output(collector, default, None);
                default.clone()
            } else if mapping.required {
                return Err(TransformError::new(
                    TransformErrorKind::MissingRequired,
                    "required value is missing",
                )
                .with_path(mapping_path));
            } else {
                return Ok(None);
            }
        }
        EvalValue::Value(value) => value,
    };

    if value.is_null() {
        if mapping.required {
            return Err(TransformError::new(
                TransformErrorKind::MissingRequired,
                "required value is null",
            )
            .with_path(mapping_path));
        }
        return Ok(Some(value));
    }

    if let Some(type_name) = &mapping.value_type {
        value = cast_value(&value, type_name, &format!("{}.type", mapping_path))?;
        collector
            .emit(TraceEventKind::TypeCast, TracePhase::Instant)
            .rule_path(format!("{}.type", mapping_path))
            .finish_with_output(collector, &value, None);
    }

    Ok(Some(value))
}

fn root_eval_locals(limits: EvalLimits) -> EvalLocals<'static> {
    EvalLocals {
        item: None,
        acc: None,
        pipe: None,
        locals: None,
        precomputed_op_args: None,
        limits,
    }
}

fn canonical_source_path(source: &str) -> String {
    match parse_source(source) {
        Ok((Namespace::Input, path)) => canonical_input_path(path),
        Ok((Namespace::Context, path)) => canonical_context_path(path),
        Ok((Namespace::Out, path)) => canonical_out_path(path),
        _ => canonical_input_path(source),
    }
}
