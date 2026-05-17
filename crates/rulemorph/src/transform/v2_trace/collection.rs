use super::*;

fn v2_eval_array_from_value(
    value: V2EvalValue,
    path: &str,
) -> Result<Vec<JsonValue>, TransformError> {
    match value {
        V2EvalValue::Missing => Ok(Vec::new()),
        V2EvalValue::Value(value) => {
            if value.is_null() {
                Ok(Vec::new())
            } else if let JsonValue::Array(items) = value {
                Ok(items)
            } else {
                Err(
                    TransformError::new(TransformErrorKind::ExprError, "expr arg must be an array")
                        .with_path(path),
                )
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_expr_or_null_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<JsonValue, TransformError> {
    match eval_v2_expr_traced(expr, record, context, out, path, ctx, collector)? {
        V2EvalValue::Missing => Ok(JsonValue::Null),
        V2EvalValue::Value(value) => Ok(value),
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_predicate_expr_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<bool, TransformError> {
    match eval_v2_expr_traced(expr, record, context, out, path, ctx, collector)? {
        V2EvalValue::Missing => Ok(false),
        V2EvalValue::Value(value) => {
            if value.is_null() {
                Ok(false)
            } else {
                value_as_bool(&value, path)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_v2_key_expr_string_traced<'a>(
    expr: &crate::v2_model::V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<String, TransformError> {
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
    value_to_string(&value, path)
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

fn emit_v2_collection_item_start(
    collector: &mut TraceCollector,
    item_path: &str,
    operator: &str,
    index: usize,
    item: &JsonValue,
) {
    collector
        .start_span(TraceEventKind::CollectionItemStart, TracePhase::Start)
        .rule_path(item_path)
        .operator(operator)
        .input_path(canonical_item_path(""))
        .attr_index("item_index", index)
        .attr_enum("scope", "item")
        .input_value(item, collector.options(), Some("@item"))
        .finish(collector);
}

fn finish_v2_collection_item(
    collector: &mut TraceCollector,
    item_path: &str,
    operator: &str,
    index: usize,
    output: &V2EvalValue,
    bool_attr: Option<(&'static str, bool)>,
) {
    let mut event = collector
        .end_span(TraceEventKind::CollectionItemEnd, TracePhase::End)
        .rule_path(item_path)
        .operator(operator)
        .attr_index("item_index", index);
    if let Some((key, value)) = bool_attr {
        event = event.attr_bool(key, value);
    }
    event.finish_with_v2_eval_output(collector, output, Some("@item"));
}

#[allow(clippy::too_many_arguments)]
pub(in crate::transform) fn eval_v2_collection_op_traced<'a>(
    op_step: &crate::v2_model::V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let step_ctx = ctx.clone().with_pipe_value(pipe_value.clone());
    let operator = op_step.op.as_str();

    match operator {
        "map" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "map requires exactly one argument",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut results = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let value = eval_v2_expr_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &value);
                finish_v2_collection_item(collector, &item_path, operator, index, &value, None);
                if let V2EvalValue::Value(value) = value {
                    results.push(value);
                }
            }
            Ok(V2EvalValue::Value(JsonValue::Array(results)))
        }
        "filter" | "partition" | "find" | "find_index" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("{operator} requires exactly one argument"),
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut kept = Vec::new();
            let mut rejected = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let matches = eval_v2_predicate_expr_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                let match_value = V2EvalValue::Value(JsonValue::Bool(matches));
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &match_value);
                finish_v2_collection_item(
                    collector,
                    &item_path,
                    operator,
                    index,
                    &match_value,
                    Some(("matched", matches)),
                );
                match operator {
                    "filter" => {
                        if matches {
                            kept.push(item.clone());
                        }
                    }
                    "partition" => {
                        if matches {
                            kept.push(item.clone());
                        } else {
                            rejected.push(item.clone());
                        }
                    }
                    "find" if matches => return Ok(V2EvalValue::Value(item.clone())),
                    "find_index" if matches => {
                        return Ok(V2EvalValue::Value(JsonValue::Number((index as i64).into())));
                    }
                    _ => {}
                }
            }
            match operator {
                "filter" => Ok(V2EvalValue::Value(JsonValue::Array(kept))),
                "partition" => Ok(V2EvalValue::Value(JsonValue::Array(vec![
                    JsonValue::Array(kept),
                    JsonValue::Array(rejected),
                ]))),
                "find" => Ok(V2EvalValue::Value(JsonValue::Null)),
                "find_index" => Ok(V2EvalValue::Value(JsonValue::Number((-1).into()))),
                _ => unreachable!(),
            }
        }
        "flat_map" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "flat_map requires exactly one argument",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut results = Vec::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let value = eval_v2_expr_or_null_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                let output = V2EvalValue::Value(value.clone());
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &output);
                finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
                match value {
                    JsonValue::Array(items) => results.extend(items),
                    value => results.push(value),
                }
            }
            Ok(V2EvalValue::Value(JsonValue::Array(results)))
        }
        "group_by" | "key_by" | "distinct_by" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    format!("{operator} requires exactly one argument"),
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let arg_path = format!("{}.args[0]", path);
            let mut grouped = serde_json::Map::new();
            let mut keyed = serde_json::Map::new();
            let mut distinct = Vec::new();
            let mut seen = HashSet::new();
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index });
                let key = eval_v2_key_expr_string_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &arg_path,
                    &item_ctx,
                    collector,
                )?;
                let key_output = V2EvalValue::Value(JsonValue::String(key.clone()));
                emit_v2_arg_eval(collector, &arg_path, 0, operator, &key_output);
                let selected = match operator {
                    "group_by" => {
                        let entry = grouped
                            .entry(key)
                            .or_insert_with(|| JsonValue::Array(Vec::new()));
                        if let JsonValue::Array(items) = entry {
                            items.push(item.clone());
                        }
                        true
                    }
                    "key_by" => {
                        keyed.insert(key, item.clone());
                        true
                    }
                    "distinct_by" => {
                        if seen.insert(key) {
                            distinct.push(item.clone());
                            true
                        } else {
                            false
                        }
                    }
                    _ => unreachable!(),
                };
                finish_v2_collection_item(
                    collector,
                    &item_path,
                    operator,
                    index,
                    &key_output,
                    Some(("selected", selected)),
                );
            }
            match operator {
                "group_by" => Ok(V2EvalValue::Value(JsonValue::Object(grouped))),
                "key_by" => Ok(V2EvalValue::Value(JsonValue::Object(keyed))),
                "distinct_by" => Ok(V2EvalValue::Value(JsonValue::Array(distinct))),
                _ => unreachable!(),
            }
        }
        "sort_by" => {
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
            let order = if op_step.args.len() == 2 {
                let order_path = format!("{}.args[1]", path);
                let order_value = eval_v2_expr_traced(
                    &op_step.args[1],
                    record,
                    context,
                    out,
                    &order_path,
                    &step_ctx,
                    collector,
                )?;
                emit_v2_arg_eval(collector, &order_path, 1, operator, &order_value);
                let order = match order_value {
                    V2EvalValue::Missing => return Ok(V2EvalValue::Missing),
                    V2EvalValue::Value(value) => value_to_string(&value, &order_path)?,
                };
                if order != "asc" && order != "desc" {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "order must be asc or desc",
                    )
                    .with_path(order_path));
                }
                order
            } else {
                "asc".to_string()
            };

            struct TracedSortItem {
                key: SortKey,
                index: usize,
                value: JsonValue,
            }

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
                finish_v2_collection_item(
                    collector,
                    &item_path,
                    operator,
                    index,
                    &key_output,
                    None,
                );
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
        "reduce" => {
            if op_step.args.len() != 1 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "reduce requires exactly one argument",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            if array.is_empty() {
                return Ok(V2EvalValue::Value(JsonValue::Null));
            }
            let expr_path = format!("{}.args[0]", path);
            let mut acc = array[0].clone();
            for (index, item) in array.iter().enumerate().skip(1) {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index })
                    .with_acc(&acc);
                let value = eval_v2_expr_or_null_traced(
                    &op_step.args[0],
                    record,
                    context,
                    out,
                    &expr_path,
                    &item_ctx,
                    collector,
                )?;
                let output = V2EvalValue::Value(value.clone());
                emit_v2_arg_eval(collector, &expr_path, 0, operator, &output);
                acc = value;
                finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
            }
            Ok(V2EvalValue::Value(acc))
        }
        "fold" => {
            if op_step.args.len() != 2 {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "fold requires exactly two arguments",
                )
                .with_path(path));
            }
            let array = v2_eval_array_from_value(pipe_value, path)?;
            let init_path = format!("{}.args[0]", path);
            let initial = eval_v2_expr_traced(
                &op_step.args[0],
                record,
                context,
                out,
                &init_path,
                &step_ctx,
                collector,
            )?;
            emit_v2_arg_eval(collector, &init_path, 0, operator, &initial);
            let mut acc = match initial {
                V2EvalValue::Missing => return Ok(V2EvalValue::Missing),
                V2EvalValue::Value(value) => value,
            };
            let expr_path = format!("{}.args[1]", path);
            for (index, item) in array.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                emit_v2_collection_item_start(collector, &item_path, operator, index, item);
                let item_ctx = step_ctx
                    .clone()
                    .with_pipe_value(V2EvalValue::Value(item.clone()))
                    .with_item(V2EvalItem { value: item, index })
                    .with_acc(&acc);
                let value = eval_v2_expr_or_null_traced(
                    &op_step.args[1],
                    record,
                    context,
                    out,
                    &expr_path,
                    &item_ctx,
                    collector,
                )?;
                let output = V2EvalValue::Value(value.clone());
                emit_v2_arg_eval(collector, &expr_path, 1, operator, &output);
                acc = value;
                finish_v2_collection_item(collector, &item_path, operator, index, &output, None);
            }
            Ok(V2EvalValue::Value(acc))
        }
        _ => eval_v2_op_step(op_step, pipe_value, record, context, out, path, ctx),
    }
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
