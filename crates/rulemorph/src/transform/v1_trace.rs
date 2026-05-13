use super::*;

pub(super) fn eval_expr_traced(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    collector
        .start_span(TraceEventKind::ExprStart, TracePhase::Start)
        .rule_path(base_path)
        .finish(collector);

    let result = match expr {
        Expr::Literal(value) => {
            let value = EvalValue::Value(value.clone());
            collector
                .emit(TraceEventKind::LiteralEval, TracePhase::Instant)
                .rule_path(base_path)
                .finish_with_eval_output(collector, &value, None);
            Ok(value)
        }
        Expr::Ref(expr_ref) => {
            let value = eval_ref(expr_ref, record, context, out, base_path, locals);
            if let Ok(value) = &value {
                collector
                    .emit(TraceEventKind::RefRead, TracePhase::Instant)
                    .rule_path(base_path)
                    .input_path(canonical_ref_path(&expr_ref.ref_path))
                    .finish_with_eval_output(collector, value, Some(&expr_ref.ref_path));
            }
            value
        }
        Expr::Op(expr_op) => {
            collector
                .start_span(TraceEventKind::OpStart, TracePhase::Start)
                .rule_path(base_path)
                .operator(&expr_op.op)
                .finish(collector);
            let op_result = match expr_op.op.as_str() {
                "coalesce" => eval_coalesce_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                "and" => eval_bool_and_or_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    true,
                    locals,
                    collector,
                ),
                "or" => eval_bool_and_or_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    false,
                    locals,
                    collector,
                ),
                "map" => eval_array_map_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                "reduce" => eval_array_reduce_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                "fold" => eval_array_fold_traced(
                    &expr_op.args,
                    None,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                    collector,
                ),
                op if v1_operator_has_scoped_expr_args(op) => {
                    eval_op(expr_op, record, context, out, base_path, None, locals)
                }
                _ => eval_eager_op_traced(
                    expr_op, record, context, out, base_path, locals, collector,
                ),
            };

            match op_result {
                Ok(value) => {
                    collector
                        .end_span(TraceEventKind::OpEnd, TracePhase::End)
                        .rule_path(base_path)
                        .operator(&expr_op.op)
                        .finish_with_eval_output(collector, &value, None);
                    Ok(value)
                }
                Err(error) => {
                    collector
                        .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
                        .rule_path(base_path)
                        .operator(&expr_op.op)
                        .finish(collector);
                    Err(error)
                }
            }
        }
        Expr::Chain(expr_chain) => eval_chain(expr_chain, record, context, out, base_path, locals),
    };

    match &result {
        Ok(value) => {
            collector
                .end_span(TraceEventKind::ExprEnd, TracePhase::End)
                .rule_path(base_path)
                .finish_with_eval_output(collector, value, None);
        }
        Err(_) => {
            collector
                .error_span(TraceEventKind::Error, "EXPR_ERROR", "expression failed")
                .rule_path(base_path)
                .finish(collector);
        }
    }

    result
}

#[allow(clippy::too_many_arguments)]
fn eval_eager_op_traced(
    expr_op: &ExprOp,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let mut arg_values = Vec::with_capacity(expr_op.args.len());
    for (arg_index, arg) in expr_op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, arg_index);
        let arg_value = eval_expr_traced(arg, record, context, out, &arg_path, locals, collector)?;
        emit_arg_eval(collector, &arg_path, arg_index, &arg_value);
        let is_missing = matches!(arg_value, EvalValue::Missing);
        arg_values.push(arg_value);
        if is_missing && v1_operator_stops_after_missing_arg(&expr_op.op) {
            break;
        }
    }
    let cached_locals = locals_with_precomputed_args(locals, base_path, &arg_values);
    eval_op(
        expr_op,
        record,
        context,
        out,
        base_path,
        None,
        Some(&cached_locals),
    )
}

fn v1_operator_has_scoped_expr_args(op: &str) -> bool {
    matches!(
        op,
        "filter"
            | "flat_map"
            | "zip_with"
            | "group_by"
            | "key_by"
            | "partition"
            | "distinct_by"
            | "sort_by"
            | "find"
            | "find_index"
    )
}

fn v1_operator_stops_after_missing_arg(op: &str) -> bool {
    matches!(
        op,
        "concat"
            | "+"
            | "-"
            | "*"
            | "/"
            | "replace"
            | "split"
            | "pad_start"
            | "pad_end"
            | "round"
            | "to_base"
            | "date_format"
            | "to_unixtime"
            | "merge"
            | "deep_merge"
            | "get"
            | "pick"
            | "omit"
            | "flatten"
            | "take"
            | "drop"
            | "slice"
            | "chunk"
            | "zip"
            | "index_of"
            | "contains"
    )
}

fn emit_arg_eval(
    collector: &mut TraceCollector,
    arg_path: &str,
    arg_index: usize,
    arg_value: &EvalValue,
) {
    collector
        .emit(TraceEventKind::ArgEval, TracePhase::Instant)
        .rule_path(arg_path)
        .attr_index("arg_index", arg_index)
        .input_eval_value(arg_value, collector.options(), None)
        .finish(collector);
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_expr_at_index_traced(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    if let Some(injected) = injected {
        if index == 0 {
            return Ok(injected.clone());
        }
        let arg = args.get(index - 1).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "expr.args index is out of bounds",
            )
            .with_path(format!("{}.args[{}]", base_path, index))
        })?;
        let arg_path = format!("{}.args[{}]", base_path, index);
        return eval_expr_traced(arg, record, context, out, &arg_path, locals, collector);
    }

    let arg = args.get(index).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[{}]", base_path, index))
    })?;
    let arg_path = format!("{}.args[{}]", base_path, index);
    eval_expr_traced(arg, record, context, out, &arg_path, locals, collector)
}

#[allow(clippy::too_many_arguments)]
fn eval_array_arg_traced(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<Vec<JsonValue>, TransformError> {
    let arg_path = format!("{}.args[{}]", base_path, index);
    let value = eval_expr_at_index_traced(
        index, args, injected, record, context, out, base_path, locals, collector,
    )?;
    emit_arg_eval(collector, &arg_path, index, &value);
    match value {
        EvalValue::Missing => Ok(Vec::new()),
        EvalValue::Value(value) => {
            if value.is_null() {
                Ok(Vec::new())
            } else if let JsonValue::Array(items) = value {
                Ok(items)
            } else {
                Err(
                    TransformError::new(TransformErrorKind::ExprError, "expr arg must be an array")
                        .with_path(arg_path),
                )
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_coalesce_traced(
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
    if total_len == 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must be a non-empty array",
        )
        .with_path(format!("{}.args", base_path)));
    }

    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index_traced(
            index, args, injected, record, context, out, base_path, locals, collector,
        )?;
        emit_arg_eval(collector, &arg_path, index, &value);
        match value {
            EvalValue::Missing => continue,
            EvalValue::Value(value) => {
                if value.is_null() {
                    continue;
                }
                return Ok(EvalValue::Value(value));
            }
        }
    }
    Ok(EvalValue::Missing)
}

#[allow(clippy::too_many_arguments)]
fn eval_array_map_traced(
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
        results.push(match value {
            EvalValue::Missing => JsonValue::Null,
            EvalValue::Value(value) => value,
        });
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

#[allow(clippy::too_many_arguments)]
fn eval_array_reduce_traced(
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
fn eval_array_fold_traced(
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
fn eval_bool_and_or_traced(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    is_and: bool,
    locals: Option<&EvalLocals<'_>>,
    collector: &mut TraceCollector,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut saw_missing = false;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index_traced(
            index, args, injected, record, context, out, base_path, locals, collector,
        )?;
        emit_arg_eval(collector, &arg_path, index, &value);
        match value {
            EvalValue::Missing => {
                saw_missing = true;
                continue;
            }
            EvalValue::Value(value) => {
                let flag = value_as_bool(&value, &arg_path)?;
                if is_and {
                    if !flag {
                        return Ok(EvalValue::Value(JsonValue::Bool(false)));
                    }
                } else if flag {
                    return Ok(EvalValue::Value(JsonValue::Bool(true)));
                }
            }
        }
    }

    if saw_missing {
        Ok(EvalValue::Missing)
    } else {
        Ok(EvalValue::Value(JsonValue::Bool(is_and)))
    }
}
