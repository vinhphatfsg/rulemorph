use std::collections::HashSet;

use super::*;

pub(super) fn eval_keyed_collection<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    match op_step.op.as_str() {
        "group_by" => eval_group_by(op_step, pipe_value, record, context, out, path, ctx),
        "key_by" => eval_key_by(op_step, pipe_value, record, context, out, path, ctx),
        "distinct_by" => eval_distinct_by(op_step, pipe_value, record, context, out, path, ctx),
        _ => unreachable!(),
    }
}

fn eval_group_by<'a>(
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
            "group_by requires exactly one argument",
        )
        .with_path(path));
    }
    let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
    let arg_path = format!("{}.args[0]", path);
    let mut results = serde_json::Map::new();
    for (index, item) in array.iter().enumerate() {
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item.clone()))
            .with_item(EvalItem { value: item, index });
        let key =
            eval_v2_key_expr_string(&op_step.args[0], record, context, out, &arg_path, &item_ctx)?;
        let entry = results
            .entry(key)
            .or_insert_with(|| JsonValue::Array(Vec::new()));
        if let JsonValue::Array(items) = entry {
            items.push(item.clone());
        }
    }
    Ok(EvalValue::Value(JsonValue::Object(results)))
}

fn eval_key_by<'a>(
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
            "key_by requires exactly one argument",
        )
        .with_path(path));
    }
    let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
    let arg_path = format!("{}.args[0]", path);
    let mut results = serde_json::Map::new();
    for (index, item) in array.iter().enumerate() {
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item.clone()))
            .with_item(EvalItem { value: item, index });
        let key =
            eval_v2_key_expr_string(&op_step.args[0], record, context, out, &arg_path, &item_ctx)?;
        results.insert(key, item.clone());
    }
    Ok(EvalValue::Value(JsonValue::Object(results)))
}

fn eval_distinct_by<'a>(
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
            "distinct_by requires exactly one argument",
        )
        .with_path(path));
    }
    let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
    let arg_path = format!("{}.args[0]", path);
    let mut results = Vec::new();
    let mut seen = HashSet::new();
    for (index, item) in array.iter().enumerate() {
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item.clone()))
            .with_item(EvalItem { value: item, index });
        let key =
            eval_v2_key_expr_string(&op_step.args[0], record, context, out, &arg_path, &item_ctx)?;
        if seen.insert(key) {
            results.push(item.clone());
        }
    }
    Ok(EvalValue::Value(JsonValue::Array(results)))
}
