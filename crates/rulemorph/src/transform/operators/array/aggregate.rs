use super::*;

pub(in crate::transform::operators) fn eval_array_sum(
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
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut sum = 0.0;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        sum += value;
    }

    Ok(EvalValue::Value(json_number_from_f64(sum, base_path)?))
}

pub(in crate::transform::operators) fn eval_array_avg(
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
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut sum = 0.0;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        sum += value;
    }
    let avg = sum / array.len() as f64;

    Ok(EvalValue::Value(json_number_from_f64(avg, base_path)?))
}

pub(in crate::transform::operators) fn eval_array_min(
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
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut min_value: Option<f64> = None;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        min_value = Some(match min_value {
            Some(current) => current.min(value),
            None => value,
        });
    }

    Ok(EvalValue::Value(json_number_from_f64(
        min_value.unwrap_or(0.0),
        base_path,
    )?))
}

pub(in crate::transform::operators) fn eval_array_max(
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
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut max_value: Option<f64> = None;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        max_value = Some(match max_value {
            Some(current) => current.max(value),
            None => value,
        });
    }

    Ok(EvalValue::Value(json_number_from_f64(
        max_value.unwrap_or(0.0),
        base_path,
    )?))
}
