use super::*;

pub(in crate::transform) struct MappingWhenInput<'a, 'ctx> {
    pub(in crate::transform) mapping: &'a crate::model::Mapping,
    pub(in crate::transform) record: &'a JsonValue,
    pub(in crate::transform) context: Option<&'a JsonValue>,
    pub(in crate::transform) out: &'a JsonValue,
    pub(in crate::transform) mapping_path: &'a str,
    pub(in crate::transform) warnings: &'a mut Vec<TransformWarning>,
    pub(in crate::transform) rule_version: u8,
    pub(in crate::transform) limits: EvalLimits,
    pub(in crate::transform) ctx: &'a V2EvalContext<'ctx>,
}

pub(in crate::transform) struct TracedMappingWhenInput<'a, 'ctx, 'collector> {
    pub(in crate::transform) eval: MappingWhenInput<'a, 'ctx>,
    pub(in crate::transform) collector: &'collector mut TraceCollector,
}

pub(in crate::transform) fn eval_when(input: MappingWhenInput<'_, '_>) -> bool {
    let MappingWhenInput {
        mapping,
        record,
        context,
        out,
        mapping_path,
        warnings,
        rule_version,
        limits,
        ctx,
    } = input;

    let expr = match &mapping.when {
        Some(expr) => expr,
        None => return true,
    };

    let when_path = format!("{}.when", mapping_path);
    match eval_when_expr_with_v2_context(
        expr,
        record,
        context,
        out,
        &when_path,
        rule_version,
        limits,
        ctx,
    ) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

pub(in crate::transform) fn eval_when_traced(input: TracedMappingWhenInput<'_, '_, '_>) -> bool {
    let TracedMappingWhenInput { eval, collector } = input;
    let MappingWhenInput {
        mapping,
        record,
        context,
        out,
        mapping_path,
        warnings,
        rule_version,
        limits,
        ctx,
    } = eval;

    let expr = match &mapping.when {
        Some(expr) => expr,
        None => return true,
    };

    let when_path = format!("{}.when", mapping_path);
    match eval_when_expr_traced_with_v2_context(
        expr,
        record,
        context,
        out,
        &when_path,
        rule_version,
        limits,
        ctx,
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
    ctx: &V2EvalContext<'_>,
) -> bool {
    let expr = match &rule.record_when {
        Some(expr) => expr,
        None => return true,
    };

    let empty_out = JsonValue::Object(Map::new());
    match eval_when_expr_with_v2_context(
        expr,
        record,
        context,
        &empty_out,
        "record_when",
        rule.version,
        limits,
        ctx,
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
    ctx: &V2EvalContext<'_>,
    collector: &mut TraceCollector,
) -> bool {
    let expr = match &rule.record_when {
        Some(expr) => expr,
        None => return true,
    };

    let empty_out = JsonValue::Object(Map::new());
    match eval_when_expr_traced_with_v2_context(
        expr,
        record,
        context,
        &empty_out,
        "record_when",
        rule.version,
        limits,
        ctx,
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

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(in crate::transform) fn eval_when_expr_with_v2_context<'a>(
    expr: &Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    rule_version: u8,
    limits: EvalLimits,
    ctx: &V2EvalContext<'a>,
) -> Result<bool, TransformError> {
    if rule_version >= 2
        && let Some(raw_value) = expr_to_json_for_v2_condition(expr)
    {
        let condition = parse_v2_condition(&raw_value).map_err(|err| {
            TransformError::new(
                TransformErrorKind::ExprError,
                format!("invalid v2 condition: {}", err),
            )
            .with_path(path)
        })?;
        let ctx = ctx.clone().with_limits(limits);
        return eval_v2_condition(&condition, record, context, out, path, &ctx);
    }

    eval_bool_expr(expr, record, context, out, path, limits)
}

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(in crate::transform) fn eval_when_expr_traced_with_v2_context<'a>(
    expr: &Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    rule_version: u8,
    limits: EvalLimits,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    if rule_version >= 2
        && let Some(raw_value) = expr_to_json_for_v2_condition(expr)
    {
        let condition = parse_v2_condition(&raw_value).map_err(|err| {
            TransformError::new(
                TransformErrorKind::ExprError,
                format!("invalid v2 condition: {}", err),
            )
            .with_path(path)
        })?;
        let ctx = ctx.clone().with_limits(limits);
        return eval_v2_condition_traced(&condition, record, context, out, path, &ctx, collector);
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
