use super::*;

pub(in crate::transform::operators) fn eval_json_get(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let base_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let base_value = match base_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if base_value.is_null() {
        return Ok(EvalValue::Missing);
    }

    let path_path = format!("{}.args[1]", base_path);
    let path_value =
        eval_expr_at_index(1, args, injected, record, context, out, base_path, locals)?;
    let path_value = match path_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if path_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path_path));
    }
    let path = value_as_string(&path_value, &path_path)?;
    if path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "path must be a non-empty string",
        )
        .with_path(path_path));
    }
    let tokens = parse_path_tokens(&path, TransformErrorKind::ExprError, &path_path)?;
    match get_path(&base_value, &tokens) {
        Some(value) => Ok(EvalValue::Value(value.clone())),
        None => Ok(EvalValue::Missing),
    }
}
