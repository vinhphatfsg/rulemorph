use super::*;

pub(in crate::transform) fn args_len(args: &[Expr], injected: Option<&EvalValue>) -> usize {
    args.len() + usize::from(injected.is_some())
}

pub(in crate::transform) fn arg_expr_at<'a>(
    index: usize,
    args: &'a [Expr],
    injected: Option<&EvalValue>,
) -> Option<&'a Expr> {
    if injected.is_some() {
        if index == 0 {
            None
        } else {
            args.get(index - 1)
        }
    } else {
        args.get(index)
    }
}

pub(super) fn eval_expr_at_index(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    if injected.is_none() {
        if let Some((cached_base_path, cached_values)) =
            locals.and_then(|locals| locals.precomputed_op_args)
        {
            if cached_base_path == base_path {
                return cached_values.get(index).cloned().ok_or_else(|| {
                    TransformError::new(
                        TransformErrorKind::ExprError,
                        "expr.args index is out of bounds",
                    )
                    .with_path(format!("{}.args[{}]", base_path, index))
                });
            }
        }
    }

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
        return eval_expr(arg, record, context, out, &arg_path, locals);
    }

    let arg = args.get(index).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[{}]", base_path, index))
    })?;
    let arg_path = format!("{}.args[{}]", base_path, index);
    eval_expr(arg, record, context, out, &arg_path, locals)
}

pub(super) fn eval_arg_value_at(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Option<JsonValue>, TransformError> {
    match eval_expr_at_index(
        index, args, injected, record, context, out, base_path, locals,
    )? {
        EvalValue::Missing => Ok(None),
        EvalValue::Value(value) => Ok(Some(value)),
    }
}

pub(super) fn eval_arg_string_at(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Option<String>, TransformError> {
    let value = match eval_arg_value_at(
        index, args, injected, record, context, out, base_path, locals,
    )? {
        None => return Ok(None),
        Some(value) => value,
    };
    let arg_path = format!("{}.args[{}]", base_path, index);
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(arg_path));
    }
    value_as_string(&value, &arg_path).map(Some)
}

pub(super) fn eval_expr_value_or_null_at(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<JsonValue, TransformError> {
    match eval_expr_at_index(
        index, args, injected, record, context, out, base_path, locals,
    )? {
        EvalValue::Missing => Ok(JsonValue::Null),
        EvalValue::Value(value) => Ok(value),
    }
}
