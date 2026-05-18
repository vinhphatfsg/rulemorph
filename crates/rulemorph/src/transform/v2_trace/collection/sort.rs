use super::*;

struct TracedSortItem {
    key: SortKey,
    index: usize,
    value: JsonValue,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::transform) fn eval_v2_sort_by_traced<'a>(
    op_step: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    step_ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let operator = "sort_by";
    if !(1..=2).contains(&op_step.args.len()) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "sort_by requires one or two arguments",
        )
        .with_path(path));
    }
    let array = v2_eval_array_from_value(pipe_value, path)?;
    if array.is_empty() {
        return Ok(V2EvalValue::Value(JsonValue::Array(Vec::new())));
    }
    let expr_path = format!("{}.args[0]", path);
    let Some(order) = eval_sort_order(op_step, record, context, out, path, step_ctx, collector)?
    else {
        return Ok(V2EvalValue::Missing);
    };

    let mut items = Vec::with_capacity(array.len());
    let mut key_kind = None;
    for (index, item) in array.iter().enumerate() {
        let item_path = format!("{}[{}]", path, index);
        emit_v2_collection_item_start(collector, &item_path, operator, index, item);
        let item_ctx = step_ctx
            .clone()
            .with_pipe_value(V2EvalValue::Value(item.clone()))
            .with_item(V2EvalItem { value: item, index });
        let key = eval_v2_sort_key_traced(
            &op_step.args[0],
            record,
            context,
            out,
            &expr_path,
            &item_ctx,
            collector,
        )?;
        let kind = key.kind();
        if let Some(existing) = key_kind {
            if existing != kind {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "sort_by keys must be all the same type",
                )
                .with_path(&expr_path));
            }
        } else {
            key_kind = Some(kind);
        }
        let key_value = sort_key_to_json(&key);
        let key_output = V2EvalValue::Value(key_value);
        emit_v2_arg_eval(collector, &expr_path, 0, operator, &key_output);
        finish_v2_collection_item(collector, &item_path, operator, index, &key_output, None);
        items.push(TracedSortItem {
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
    Ok(V2EvalValue::Value(JsonValue::Array(
        items.into_iter().map(|item| item.value).collect(),
    )))
}

#[allow(clippy::too_many_arguments)]
fn eval_sort_order<'a>(
    op_step: &crate::v2_model::V2OpStep,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    step_ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<Option<String>, TransformError> {
    if op_step.args.len() != 2 {
        return Ok(Some("asc".to_string()));
    }

    let operator = "sort_by";
    let order_path = format!("{}.args[1]", path);
    let order_value = eval_v2_expr_traced(
        &op_step.args[1],
        record,
        context,
        out,
        &order_path,
        step_ctx,
        collector,
    )?;
    emit_v2_arg_eval(collector, &order_path, 1, operator, &order_value);
    let order = match order_value {
        V2EvalValue::Missing => return Ok(None),
        V2EvalValue::Value(value) => value_to_string(&value, &order_path)?,
    };
    if order != "asc" && order != "desc" {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "order must be asc or desc",
        )
        .with_path(order_path));
    }
    Ok(Some(order))
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_sort_key_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<SortKey, TransformError> {
    let value = match eval_v2_expr_traced(expr, record, context, out, path, ctx, collector)? {
        V2EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(path));
        }
        V2EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path));
    }
    sort_key_from_value(&value, path)
}

pub(in crate::transform) fn sort_key_to_json(key: &SortKey) -> JsonValue {
    match key {
        SortKey::Number(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        SortKey::String(value) => JsonValue::String(value.clone()),
        SortKey::Bool(value) => JsonValue::Bool(*value),
    }
}
