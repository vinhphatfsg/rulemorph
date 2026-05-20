use super::*;

pub(in crate::transform::operators) fn eval_array_zip(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut arrays = Vec::new();
    for index in 0..total_len {
        arrays.push(eval_array_arg(
            index, args, injected, record, context, out, base_path, locals,
        )?);
    }

    let min_len = arrays.iter().map(|items| items.len()).min().unwrap_or(0);
    let mut results = Vec::with_capacity(min_len);
    for idx in 0..min_len {
        let mut row = Vec::with_capacity(arrays.len());
        for array in &arrays {
            row.push(array[idx].clone());
        }
        results.push(JsonValue::Array(row));
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(in crate::transform::operators) fn eval_array_zip_with(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 3 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let expr_index = total_len - 1;
    let expr = arg_expr_at(expr_index, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[{}]", base_path, expr_index))
    })?;
    let expr_arg_index = if injected.is_some() {
        expr_index - 1
    } else {
        expr_index
    };
    let expr_path = format!("{}.args[{}]", base_path, expr_arg_index);

    let mut arrays = Vec::new();
    for index in 0..expr_index {
        arrays.push(eval_array_arg(
            index, args, injected, record, context, out, base_path, locals,
        )?);
    }

    let min_len = arrays.iter().map(|items| items.len()).min().unwrap_or(0);
    let mut results = Vec::with_capacity(min_len);
    for idx in 0..min_len {
        let mut row = Vec::with_capacity(arrays.len());
        for array in &arrays {
            row.push(array[idx].clone());
        }
        let row_value = JsonValue::Array(row);
        let item_locals = locals_with_item(
            locals,
            EvalItem {
                value: &row_value,
                index: idx,
            },
        );
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.push(value);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(in crate::transform::operators) fn eval_array_unzip(
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
        return Ok(EvalValue::Value(JsonValue::Array(Vec::new())));
    }

    let mut columns: Vec<Vec<JsonValue>> = Vec::new();
    let mut expected_len: Option<usize> = None;
    for item in &array {
        let items = match item {
            JsonValue::Array(items) => items,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "unzip items must be arrays",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
        };
        if let Some(expected) = expected_len {
            if items.len() != expected {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "unzip items must have the same length",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
        } else {
            expected_len = Some(items.len());
            columns = vec![Vec::with_capacity(array.len()); items.len()];
        }
        for (index, value) in items.iter().enumerate() {
            if let Some(column) = columns.get_mut(index) {
                column.push(value.clone());
            }
        }
    }

    let output = columns
        .into_iter()
        .map(JsonValue::Array)
        .collect::<Vec<_>>();
    Ok(EvalValue::Value(JsonValue::Array(output)))
}
