use super::*;

pub(super) fn eval_filter<'a>(
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
            "filter requires exactly one argument",
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
        if eval_v2_predicate_expr(&op_step.args[0], record, context, out, &arg_path, &item_ctx)? {
            results.push(item.clone());
        }
    }
    Ok(EvalValue::Value(JsonValue::Array(results)))
}

pub(super) fn eval_partition<'a>(
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
            "partition requires exactly one argument",
        )
        .with_path(path));
    }
    let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
    let arg_path = format!("{}.args[0]", path);
    let mut matched = Vec::new();
    let mut unmatched = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item.clone()))
            .with_item(EvalItem { value: item, index });
        if eval_v2_predicate_expr(&op_step.args[0], record, context, out, &arg_path, &item_ctx)? {
            matched.push(item.clone());
        } else {
            unmatched.push(item.clone());
        }
    }
    Ok(EvalValue::Value(JsonValue::Array(vec![
        JsonValue::Array(matched),
        JsonValue::Array(unmatched),
    ])))
}

pub(super) fn eval_find<'a>(
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
            "find requires exactly one argument",
        )
        .with_path(path));
    }
    let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
    let arg_path = format!("{}.args[0]", path);
    for (index, item) in array.iter().enumerate() {
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item.clone()))
            .with_item(EvalItem { value: item, index });
        if eval_v2_predicate_expr(&op_step.args[0], record, context, out, &arg_path, &item_ctx)? {
            return Ok(EvalValue::Value(item.clone()));
        }
    }
    Ok(EvalValue::Value(JsonValue::Null))
}

pub(super) fn eval_find_index<'a>(
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
            "find_index requires exactly one argument",
        )
        .with_path(path));
    }
    let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
    let arg_path = format!("{}.args[0]", path);
    for (index, item) in array.iter().enumerate() {
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item.clone()))
            .with_item(EvalItem { value: item, index });
        if eval_v2_predicate_expr(&op_step.args[0], record, context, out, &arg_path, &item_ctx)? {
            return Ok(EvalValue::Value(JsonValue::Number((index as i64).into())));
        }
    }
    Ok(EvalValue::Value(JsonValue::Number((-1).into())))
}
