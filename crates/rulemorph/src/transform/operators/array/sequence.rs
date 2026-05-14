use super::*;

fn flatten_value(value: &JsonValue, depth: usize, out: &mut Vec<JsonValue>) {
    if depth == 0 {
        out.push(value.clone());
        return;
    }

    if let JsonValue::Array(items) = value {
        for item in items {
            flatten_value(item, depth - 1, out);
        }
    } else {
        out.push(value.clone());
    }
}

pub(in crate::transform::operators) fn eval_array_flatten(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=2).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one or two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let depth = if total_len == 2 {
        let depth_path = format!("{}.args[1]", base_path);
        let depth_value =
            match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if depth_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(depth_path));
        }
        let depth = value_to_i64(
            &depth_value,
            &depth_path,
            "depth must be a non-negative integer",
        )?;
        if depth < 0 {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "depth must be a non-negative integer",
            )
            .with_path(depth_path));
        }
        usize::try_from(depth).map_err(|_| {
            TransformError::new(TransformErrorKind::ExprError, "depth is too large")
                .with_path(depth_path)
        })?
    } else {
        1
    };

    let mut results = Vec::new();
    for item in &array {
        flatten_value(item, depth, &mut results);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(in crate::transform::operators) fn eval_array_take(
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
    let count_path = format!("{}.args[1]", base_path);
    let count_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if count_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(count_path));
    }
    let count = value_to_i64(&count_value, &count_path, "count must be an integer")?;

    let len = array.len() as i64;
    let results = if count >= 0 {
        let take_count = count.min(len).max(0) as usize;
        array[..take_count].to_vec()
    } else {
        let abs_count = if count == i64::MIN {
            (i64::MAX as u64) + 1
        } else {
            (-count) as u64
        };
        let take_count = abs_count.min(array.len() as u64) as usize;
        let start = array.len().saturating_sub(take_count);
        array[start..].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(in crate::transform::operators) fn eval_array_drop(
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
    let count_path = format!("{}.args[1]", base_path);
    let count_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if count_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(count_path));
    }
    let count = value_to_i64(&count_value, &count_path, "count must be an integer")?;

    let len = array.len() as i64;
    let results = if count >= 0 {
        let drop_count = count.min(len).max(0) as usize;
        array[drop_count..].to_vec()
    } else {
        let abs_count = if count == i64::MIN {
            (i64::MAX as u64) + 1
        } else {
            (-count) as u64
        };
        let drop_count = abs_count.min(array.len() as u64) as usize;
        let end = array.len().saturating_sub(drop_count);
        array[..end].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(in crate::transform::operators) fn eval_array_slice(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two or three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let len = array.len() as i64;

    let start_path = format!("{}.args[1]", base_path);
    let start_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if start_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(start_path));
    }
    let start = value_to_i64(&start_value, &start_path, "start must be an integer")?;

    let end = if total_len == 3 {
        let end_path = format!("{}.args[2]", base_path);
        let end_value =
            match eval_arg_value_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if end_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(end_path));
        }
        value_to_i64(&end_value, &end_path, "end must be an integer")?
    } else {
        len
    };

    let mut start_index = if start < 0 { len + start } else { start };
    let mut end_index = if end < 0 { len + end } else { end };
    start_index = start_index.clamp(0, len);
    end_index = end_index.clamp(0, len);

    let results = if end_index <= start_index {
        Vec::new()
    } else {
        array[start_index as usize..end_index as usize].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(in crate::transform::operators) fn eval_array_chunk(
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
    let size_path = format!("{}.args[1]", base_path);
    let size_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if size_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(size_path));
    }
    let size = value_to_i64(&size_value, &size_path, "size must be a positive integer")?;
    if size <= 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "size must be a positive integer",
        )
        .with_path(size_path));
    }
    let size = usize::try_from(size).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "size is too large").with_path(size_path)
    })?;

    let mut chunks = Vec::new();
    let mut index = 0;
    while index < array.len() {
        let end = (index + size).min(array.len());
        chunks.push(JsonValue::Array(array[index..end].to_vec()));
        index = end;
    }

    Ok(EvalValue::Value(JsonValue::Array(chunks)))
}

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
