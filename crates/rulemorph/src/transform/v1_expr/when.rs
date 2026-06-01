use super::*;

pub(in crate::transform) fn eval_when(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    limits: EvalLimits,
) -> bool {
    let expr = match &mapping.when {
        Some(expr) => expr,
        None => return true,
    };

    let when_path = format!("{}.when", mapping_path);
    match eval_when_expr(expr, record, context, out, &when_path, rule_version, limits) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::transform) fn eval_when_traced(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    limits: EvalLimits,
    collector: &mut TraceCollector,
) -> bool {
    let expr = match &mapping.when {
        Some(expr) => expr,
        None => return true,
    };

    let when_path = format!("{}.when", mapping_path);
    match eval_when_expr_traced(
        expr,
        record,
        context,
        out,
        &when_path,
        rule_version,
        limits,
        collector,
    ) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

pub(in crate::transform) fn eval_record_when(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    limits: EvalLimits,
) -> bool {
    let expr = match &rule.record_when {
        Some(expr) => expr,
        None => return true,
    };

    let empty_out = JsonValue::Object(Map::new());
    match eval_when_expr(
        expr,
        record,
        context,
        &empty_out,
        "record_when",
        rule.version,
        limits,
    ) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

pub(in crate::transform) fn eval_record_when_traced(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    limits: EvalLimits,
    collector: &mut TraceCollector,
) -> bool {
    let expr = match &rule.record_when {
        Some(expr) => expr,
        None => return true,
    };

    let empty_out = JsonValue::Object(Map::new());
    match eval_when_expr_traced(
        expr,
        record,
        context,
        &empty_out,
        "record_when",
        rule.version,
        limits,
        collector,
    ) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

fn eval_bool_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    limits: EvalLimits,
) -> Result<bool, TransformError> {
    let locals = root_eval_locals(limits);
    let value = eval_expr(expr, record, context, out, path, Some(&locals))?;
    let value = match value {
        EvalValue::Missing => JsonValue::Null,
        EvalValue::Value(value) => value,
    };
    match value {
        JsonValue::Bool(flag) => Ok(flag),
        _ => Err(when_type_error(path)),
    }
}

fn eval_bool_expr_traced(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    limits: EvalLimits,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    let locals = root_eval_locals(limits);
    let value = eval_expr_traced(expr, record, context, out, path, Some(&locals), collector)?;
    let value = match value {
        EvalValue::Missing => JsonValue::Null,
        EvalValue::Value(value) => value,
    };
    match value {
        JsonValue::Bool(flag) => Ok(flag),
        _ => Err(when_type_error(path)),
    }
}

pub(in crate::transform) fn eval_when_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    rule_version: u8,
    limits: EvalLimits,
) -> Result<bool, TransformError> {
    if rule_version >= 2 {
        if let Some(raw_value) = expr_to_json_for_v2_condition(expr) {
            let condition = parse_v2_condition(&raw_value).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("invalid v2 condition: {}", err),
                )
                .with_path(path)
            })?;
            let ctx = V2EvalContext::new().with_limits(limits);
            return eval_v2_condition(&condition, record, context, out, path, &ctx);
        }
    }

    eval_bool_expr(expr, record, context, out, path, limits)
}

pub(in crate::transform) fn eval_when_expr_traced(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
    rule_version: u8,
    limits: EvalLimits,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    if rule_version >= 2 {
        if let Some(raw_value) = expr_to_json_for_v2_condition(expr) {
            let condition = parse_v2_condition(&raw_value).map_err(|err| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("invalid v2 condition: {}", err),
                )
                .with_path(path)
            })?;
            let ctx = V2EvalContext::new().with_limits(limits);
            return eval_v2_condition_traced(
                &condition, record, context, out, path, &ctx, collector,
            );
        }
    }

    eval_bool_expr_traced(expr, record, context, out, path, limits, collector)
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

fn when_type_error(path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::ExprError,
        "when/record_when must evaluate to boolean",
    )
    .with_path(path)
}
