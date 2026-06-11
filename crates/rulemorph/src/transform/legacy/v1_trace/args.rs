use super::*;

pub(super) fn eval_eager_op_traced(
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

pub(super) fn v1_operator_has_scoped_expr_args(op: &str) -> bool {
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
            | "abs"
            | "floor"
            | "ceil"
            | "trunc"
            | "sqrt"
            | "sign"
            | "mod"
            | "pow"
            | "clamp"
            | "range"
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

pub(super) fn emit_arg_eval(
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

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
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

#[expect(
    clippy::too_many_arguments,
    reason = "existing eval and trace helpers retain their shared call shape until a wider context rewrite"
)]
pub(super) fn eval_array_arg_traced(
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
