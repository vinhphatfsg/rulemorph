use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_array_map_traced(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg_traced(
        0, args, injected, record, context, out, base_path, locals, collector,
    )?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let limits = locals.map(|locals| locals.limits).unwrap_or_default();
    let mut generated_items = 0usize;
    let mut results = Vec::with_capacity(array.len());
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let value = eval_expr_traced(
            expr,
            record,
            context,
            out,
            &expr_path,
            Some(&item_locals),
            collector,
        )?;
        emit_arg_eval(collector, &expr_path, expr_index, &value);
        let value = match value {
            EvalValue::Missing => JsonValue::Null,
            EvalValue::Value(value) => value,
        };
        push_generated_array_item(&mut results, value, limits, base_path, &mut generated_items)?;
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_array_reduce_traced(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg_traced(
        0, args, injected, record, context, out, base_path, locals, collector,
    )?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut acc = array[0].clone();
    for (index, item) in array.iter().enumerate().skip(1) {
        let item_locals = EvalLocals {
            item: Some(EvalItem { value: item, index }),
            acc: Some(&acc),
            pipe: locals.and_then(|locals| locals.pipe),
            locals: locals.and_then(|locals| locals.locals),
            precomputed_op_args: None,
            limits: locals.map(|locals| locals.limits).unwrap_or_default(),
        };
        let value = eval_expr_traced(
            expr,
            record,
            context,
            out,
            &expr_path,
            Some(&item_locals),
            collector,
        )?;
        emit_arg_eval(collector, &expr_path, expr_index, &value);
        acc = match value {
            EvalValue::Missing => JsonValue::Null,
            EvalValue::Value(value) => value,
        };
    }

    Ok(EvalValue::Value(acc))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_array_fold_traced(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 3 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg_traced(
        0, args, injected, record, context, out, base_path, locals, collector,
    )?;
    let initial_path = format!("{}.args[1]", base_path);
    let initial = eval_expr_at_index_traced(
        1, args, injected, record, context, out, base_path, locals, collector,
    )?;
    emit_arg_eval(collector, &initial_path, 1, &initial);
    let mut acc = match initial {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };

    let expr = arg_expr_at(2, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[2]", base_path))
    })?;
    let expr_index = if injected.is_some() { 1 } else { 2 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    for (index, item) in array.iter().enumerate() {
        let item_locals = EvalLocals {
            item: Some(EvalItem { value: item, index }),
            acc: Some(&acc),
            pipe: locals.and_then(|locals| locals.pipe),
            locals: locals.and_then(|locals| locals.locals),
            precomputed_op_args: None,
            limits: locals.map(|locals| locals.limits).unwrap_or_default(),
        };
        let value = eval_expr_traced(
            expr,
            record,
            context,
            out,
            &expr_path,
            Some(&item_locals),
            collector,
        )?;
        emit_arg_eval(collector, &expr_path, expr_index, &value);
        acc = match value {
            EvalValue::Missing => JsonValue::Null,
            EvalValue::Value(value) => value,
        };
    }

    Ok(EvalValue::Value(acc))
}
