use super::*;

pub(in crate::transform::operators) fn eval_array_unique(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let item_path = format!("{}.args[0]", base_path);

    let mut results: Vec<JsonValue> = Vec::new();
    for item in array {
        ensure_eq_compatible(&item, &item_path)?;
        let mut exists = false;
        for existing in &results {
            if compare_eq(&item, existing, &item_path, &item_path)? {
                exists = true;
                break;
            }
        }
        if !exists {
            results.push(item);
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(in crate::transform::operators) fn eval_array_index_of(
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

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let value_path = format!("{}.args[1]", base_path);
    let value =
        eval_expr_value_or_null_at(1, args, injected, record, context, out, base_path, locals)?;

    ensure_eq_compatible(&value, &value_path)?;
    let item_path = format!("{}.args[0]", base_path);
    for (index, item) in array.iter().enumerate() {
        ensure_eq_compatible(item, &item_path)?;
        if compare_eq(item, &value, &item_path, &value_path)? {
            return Ok(EvalValue::Value(JsonValue::Number((index as i64).into())));
        }
    }

    Ok(EvalValue::Value(JsonValue::Number((-1).into())))
}

pub(in crate::transform::operators) fn eval_array_contains(
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

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let value_path = format!("{}.args[1]", base_path);
    let value =
        eval_expr_value_or_null_at(1, args, injected, record, context, out, base_path, locals)?;

    ensure_eq_compatible(&value, &value_path)?;
    let item_path = format!("{}.args[0]", base_path);
    for item in &array {
        ensure_eq_compatible(item, &item_path)?;
        if compare_eq(item, &value, &item_path, &value_path)? {
            return Ok(EvalValue::Value(JsonValue::Bool(true)));
        }
    }

    Ok(EvalValue::Value(JsonValue::Bool(false)))
}
