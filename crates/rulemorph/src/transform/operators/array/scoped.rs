use super::*;

mod accumulator;
pub(super) mod predicate;

pub(in crate::transform::operators) use self::accumulator::{eval_array_fold, eval_array_reduce};

pub(in crate::transform::operators) fn eval_array_map(
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
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.push(value);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(in crate::transform::operators) fn eval_array_flat_map(
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
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        match value {
            JsonValue::Array(items) => results.extend(items),
            value => results.push(value),
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(in crate::transform::operators) fn eval_array_group_by(
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
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Map::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        let entry = results
            .entry(key)
            .or_insert_with(|| JsonValue::Array(Vec::new()));
        if let JsonValue::Array(items) = entry {
            items.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Object(results)))
}

pub(in crate::transform::operators) fn eval_array_key_by(
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
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Map::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.insert(key, item.clone());
    }

    Ok(EvalValue::Value(JsonValue::Object(results)))
}

pub(in crate::transform::operators) fn eval_array_distinct_by(
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
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::new();
    let mut seen = HashSet::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        if seen.insert(key) {
            results.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(in crate::transform::operators) fn eval_array_sort_by(
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
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Array(Vec::new())));
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

    let order = if total_len == 3 {
        let order_path = format!("{}.args[2]", base_path);
        let value =
            match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if value != "asc" && value != "desc" {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "order must be asc or desc",
            )
            .with_path(order_path));
        }
        value
    } else {
        "asc".to_string()
    };

    struct SortItem {
        key: SortKey,
        index: usize,
        value: JsonValue,
    }

    let mut items = Vec::with_capacity(array.len());
    let mut key_kind: Option<SortKeyKind> = None;
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_sort_key(expr, record, context, out, &expr_path, Some(&item_locals))?;
        let kind = key.kind();
        if let Some(existing) = key_kind {
            if existing != kind {
                return Err(expr_type_error(
                    "sort_by keys must be all the same type",
                    &expr_path,
                ));
            }
        } else {
            key_kind = Some(kind);
        }
        items.push(SortItem {
            key,
            index,
            value: item.clone(),
        });
    }

    items.sort_by(|left, right| {
        let mut ordering = compare_sort_keys(&left.key, &right.key);
        if order == "desc" {
            ordering = ordering.reverse();
        }
        if ordering == Ordering::Equal {
            left.index.cmp(&right.index)
        } else {
            ordering
        }
    });

    let results = items.into_iter().map(|item| item.value).collect::<Vec<_>>();
    Ok(EvalValue::Value(JsonValue::Array(results)))
}
