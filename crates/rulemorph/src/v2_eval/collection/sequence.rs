use super::*;

pub(super) fn eval_map<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    if op_step.args.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "map requires exactly one argument",
        )
        .with_path(path));
    }
    let array = match pipe_value {
        EvalValue::Missing => {
            return Ok(EvalValue::Missing);
        }
        EvalValue::Value(JsonValue::Array(items)) => items,
        EvalValue::Value(other) => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                format!("expr arg must be an array, got {:?}", other),
            )
            .with_path(path));
        }
    };
    let arg_path = format!("{}.args[0]", path);
    let mut results = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item.clone()))
            .with_item(EvalItem { value: item, index });
        let value = eval_v2_expr(&op_step.args[0], record, context, out, &arg_path, &item_ctx)?;
        if let EvalValue::Value(value) = value {
            results.push(value);
        }
    }
    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(super) fn eval_flat_map<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    if op_step.args.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "flat_map requires exactly one argument",
        )
        .with_path(path));
    }
    let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
    let arg_path = format!("{}.args[0]", path);
    let mut results = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item.clone()))
            .with_item(EvalItem { value: item, index });
        let value =
            eval_v2_expr_or_null(&op_step.args[0], record, context, out, &arg_path, &item_ctx)?;
        match value {
            JsonValue::Array(items) => results.extend(items),
            value => results.push(value),
        }
    }
    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(super) fn eval_zip_with<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    if op_step.args.len() < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "zip_with requires at least two arguments",
        )
        .with_path(path));
    }
    let mut arrays = Vec::new();
    arrays.push(eval_v2_array_from_eval_value(pipe_value.clone(), path)?);
    for (index, arg) in op_step.args.iter().enumerate().take(op_step.args.len() - 1) {
        let arg_path = format!("{}.args[{}]", path, index);
        let value = eval_v2_expr(arg, record, context, out, &arg_path, ctx)?;
        arrays.push(eval_v2_array_from_eval_value(value, &arg_path)?);
    }

    let min_len = arrays.iter().map(|items| items.len()).min().unwrap_or(0);
    let expr_index = op_step.args.len() - 1;
    let expr_path = format!("{}.args[{}]", path, expr_index);
    let expr = &op_step.args[expr_index];
    let mut results = Vec::with_capacity(min_len);
    for row_index in 0..min_len {
        let mut row = Vec::with_capacity(arrays.len());
        for array in &arrays {
            row.push(array[row_index].clone());
        }
        let row_value = JsonValue::Array(row);
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(row_value.clone()))
            .with_item(EvalItem {
                value: &row_value,
                index: row_index,
            });
        let value = eval_v2_expr_or_null(expr, record, context, out, &expr_path, &item_ctx)?;
        results.push(value);
    }
    Ok(EvalValue::Value(JsonValue::Array(results)))
}
